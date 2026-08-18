#include "elysiumkv/dynamo_manifest_catalog.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/Array.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/BatchWriteItemRequest.h>
#include <aws/dynamodb/model/CreateTableRequest.h>
#include <aws/dynamodb/model/DeleteRequest.h>
#include <aws/dynamodb/model/DescribeTableRequest.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <aws/dynamodb/model/KeySchemaElement.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/QueryRequest.h>
#include <aws/dynamodb/model/UpdateItemRequest.h>
#include <aws/dynamodb/model/WriteRequest.h>

#include <zstd.h>

#include <algorithm>
#include <charconv>
#include <cstdio>
#include <mutex>
#include <thread>
#include <utility>

namespace elysiumkv {
namespace {

class SdkGuard {
public:
    SdkGuard() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (refs_++ == 0) {
            options_ = new Aws::SDKOptions();
            Aws::InitAPI(*options_);
        }
    }
    ~SdkGuard() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (--refs_ == 0) {
            Aws::ShutdownAPI(*options_);
            delete options_;
            options_ = nullptr;
        }
    }
    SdkGuard(const SdkGuard&) = delete;
    SdkGuard& operator=(const SdkGuard&) = delete;

private:
    static std::mutex mutex_;
    static int refs_;
    static Aws::SDKOptions* options_;
};

std::mutex SdkGuard::mutex_;
int SdkGuard::refs_ = 0;
Aws::SDKOptions* SdkGuard::options_ = nullptr;

namespace attr {
constexpr const char* kPk = "PK";
constexpr const char* kSk = "SK";
constexpr const char* kVersion = "version";
constexpr const char* kGeneration = "generation";
constexpr const char* kPayload = "payload";
constexpr const char* kTotalChunks = "total_chunks";
}  // namespace attr

constexpr const char* kPointerSk = "POINTER";

/// An item caps at **400 KB including attribute names and keys**, so the payload
/// budget has to sit well below it. 300 KB leaves room for the key, the sort key
/// and the bookkeeping attributes without arithmetic that has to be revisited
/// whenever one is added.
constexpr size_t kChunkPayloadBytes = 300 * 1024;

std::string sort_key_snapshot(uint64_t generation, uint32_t chunk) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "gen#%012llu#snap#%04u",
                  static_cast<unsigned long long>(generation), chunk);
    return buf;
}

/// **The chunk index is new, so an edit written by an earlier build is unreadable here.** That
/// costs nothing as long as this ships inside the same release as manifest format 6, which already
/// requires every existing store to be rebuilt from its log — the engine takes a manifest version
/// as a clean break and never dual-reads. Landing it after 0.7.0 would need a bump of its own.
std::string sort_key_edit(uint64_t generation, uint64_t seq, uint32_t chunk) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "gen#%012llu#edit#%012llu#%04u",
                  static_cast<unsigned long long>(generation),
                  static_cast<unsigned long long>(seq), chunk);
    return buf;
}

/// Every chunk of one edit. The trailing separator is what keeps seq 1 from matching seq 10.
std::string edit_chunk_prefix(uint64_t generation, uint64_t seq) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "gen#%012llu#edit#%012llu#",
                  static_cast<unsigned long long>(generation),
                  static_cast<unsigned long long>(seq));
    return buf;
}

/// The marker `list_edits` reads the sequence number after. Parsing from the *left* rather than
/// with `rfind('#')`, which now finds the chunk index instead.
constexpr std::string_view kEditMarker = "#edit#";

std::string edit_prefix(uint64_t generation) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "gen#%012llu#edit#",
                  static_cast<unsigned long long>(generation));
    return buf;
}

std::string snapshot_prefix(uint64_t generation) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "gen#%012llu#snap#",
                  static_cast<unsigned long long>(generation));
    return buf;
}

std::string generation_prefix(uint64_t generation) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "gen#%012llu#",
                  static_cast<unsigned long long>(generation));
    return buf;
}

Status compress(Slice content, std::string& out) {
    const size_t bound = ZSTD_compressBound(content.size());
    out.resize(8 + bound);
    const uint64_t original = content.size();
    for (int i = 0; i < 8; ++i) out[static_cast<size_t>(i)] = static_cast<char>(original >> (8 * i));
    const size_t written = ZSTD_compress(out.data() + 8, bound, content.data(), content.size(), 3);
    if (ZSTD_isError(written) != 0) return Status::Io;
    out.resize(8 + written);
    return Status::Ok;
}

GetResult decompress(const std::string& raw) {
    if (raw.size() < 8) return std::unexpected(Status::Corrupt);
    uint64_t original = 0;
    for (int i = 0; i < 8; ++i) {
        original |= static_cast<uint64_t>(static_cast<unsigned char>(raw[static_cast<size_t>(i)]))
                    << (8 * i);
    }
    if (original > (1ull << 30)) return std::unexpected(Status::Corrupt);

    Buffer out(original);
    if (original > 0) {
        const size_t produced =
            ZSTD_decompress(out.data(), original, raw.data() + 8, raw.size() - 8);
        if (ZSTD_isError(produced) != 0 || produced != original) {
            return std::unexpected(Status::Corrupt);
        }
    }
    return out;
}

bool is_conditional_failure(const Aws::DynamoDB::DynamoDBError& error) {
    return error.GetErrorType() ==
           Aws::DynamoDB::DynamoDBErrors::CONDITIONAL_CHECK_FAILED;
}

/// The request itself was refused — the wrong shape, or an item over the 400 KB cap. Retrying it
/// unchanged cannot succeed, which is the whole difference that matters here.
bool is_validation_failure(const Aws::DynamoDB::DynamoDBError& error) {
    return error.GetErrorType() == Aws::DynamoDB::DynamoDBErrors::VALIDATION;
}

}  // namespace

struct DynamoManifestCatalog::Impl {
    SdkGuard sdk;
    DynamoOptions options;
    std::shared_ptr<Aws::DynamoDB::DynamoDBClient> client;

    Aws::DynamoDB::Model::AttributeValue pk() const {
        return Aws::DynamoDB::Model::AttributeValue(options.store_id);
    }

    /// Write-once by condition, not by convention: a put at an existing address is
    /// a programming error per ARCHITECTURE.md "Ownership is one compare-and-set", and `attribute_not_exists` is what makes it
    /// fail rather than silently replace.
    Status put_once(const std::string& sort_key, const std::string& payload,
                    std::optional<uint32_t> total_chunks) {
        Aws::DynamoDB::Model::PutItemRequest request;
        request.SetTableName(options.table);
        request.AddItem(attr::kPk, pk());
        request.AddItem(attr::kSk, Aws::DynamoDB::Model::AttributeValue(sort_key));

        Aws::DynamoDB::Model::AttributeValue body;
        body.SetB(Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(payload.data()),
                                         payload.size()));
        request.AddItem(attr::kPayload, body);
        if (total_chunks.has_value()) {
            request.AddItem(attr::kTotalChunks, Aws::DynamoDB::Model::AttributeValue().SetN(
                                                    std::to_string(*total_chunks)));
        }
        request.SetConditionExpression("attribute_not_exists(SK)");

        auto outcome = client->PutItem(request);
        if (outcome.IsSuccess()) return Status::Ok;
        if (is_conditional_failure(outcome.GetError())) return Status::Config;
        // **Terminal, not retryable.** A rejected *request* — an item over the 400 KB cap, a table
        // whose schema does not match — will be rejected identically forever, and `Status::Io`
        // means "ask again later" to everything above. That turns a permanent failure into a
        // background retry loop whose only symptom is `background_failures` climbing.
        if (is_validation_failure(outcome.GetError())) return Status::Config;
        return Status::Io;
    }

    /// Compresses, splits and writes `bytes` as chunks addressed by `sort_key(index)`.
    ///
    /// **Chunked unconditionally, not above a threshold.** Items cap at 400 KB, and a threshold
    /// would mean the chunking path only ever runs in production, where it is least welcome to be
    /// wrong.
    Status put_chunked(const std::function<std::string(uint32_t)>& sort_key, Slice bytes) {
        std::string packed;
        if (const Status status = compress(bytes, packed); status != Status::Ok) return status;

        const uint32_t total =
            static_cast<uint32_t>((packed.size() + kChunkPayloadBytes - 1) / kChunkPayloadBytes);
        const uint32_t chunks = std::max<uint32_t>(total, 1);

        for (uint32_t index = 0; index < chunks; ++index) {
            const size_t offset = static_cast<size_t>(index) * kChunkPayloadBytes;
            const size_t length = std::min(kChunkPayloadBytes, packed.size() - offset);
            const Status status =
                put_once(sort_key(index), packed.substr(offset, length), chunks);
            if (status != Status::Ok) return status;
        }
        return Status::Ok;
    }

    /// Reads back what `put_chunked` wrote under `prefix`.
    GetResult get_chunked(const std::string& prefix) {
        auto items = query_prefix(prefix);
        if (!items) return std::unexpected(items.error());
        if (items->empty()) return std::unexpected(Status::NotFound);

        // Sorting by sort key puts the chunks back in order; the zero-padded index is
        // what makes lexicographic order the right order.
        std::sort(items->begin(), items->end(), [](const auto& a, const auto& b) {
            return a.at(attr::kSk).GetS() < b.at(attr::kSk).GetS();
        });

        uint32_t declared = 0;
        const auto total = items->front().find(attr::kTotalChunks);
        if (total != items->front().end()) {
            const std::string& text = total->second.GetN();
            std::from_chars(text.data(), text.data() + text.size(), declared);
        }
        // A short chunk set is a half-written record — an orphan whose pointer was never
        // installed. Reporting it as Corrupt rather than silently concatenating what is there is
        // the difference between a loud failure and a truncated manifest.
        if (declared != 0 && declared != items->size()) return std::unexpected(Status::Corrupt);

        std::string packed;
        for (const auto& item : *items) {
            const auto payload = item.find(attr::kPayload);
            if (payload == item.end()) return std::unexpected(Status::Corrupt);
            const auto& bytes = payload->second.GetB();
            packed.append(reinterpret_cast<const char*>(bytes.GetUnderlyingData()),
                          bytes.GetLength());
        }
        return decompress(packed);
    }

    Result<std::vector<Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>>> query_prefix(
        const std::string& prefix) {
        std::vector<Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue>> items;
        Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> start;
        do {
            Aws::DynamoDB::Model::QueryRequest request;
            request.SetTableName(options.table);
            request.SetKeyConditionExpression("PK = :pk AND begins_with(SK, :sk)");
            request.AddExpressionAttributeValues(":pk", pk());
            request.AddExpressionAttributeValues(
                ":sk", Aws::DynamoDB::Model::AttributeValue(prefix));
            if (!start.empty()) request.SetExclusiveStartKey(start);

            auto outcome = client->Query(request);
            if (!outcome.IsSuccess()) return std::unexpected(Status::Io);
            for (const auto& item : outcome.GetResult().GetItems()) items.push_back(item);
            // A Query caps at 1 MB per page, so a chunked snapshot spans pages by
            // construction. Reading only the first would silently truncate it.
            start = outcome.GetResult().GetLastEvaluatedKey();
        } while (!start.empty());
        return items;
    }

    Status ensure_table() {
        Aws::DynamoDB::Model::DescribeTableRequest describe;
        describe.SetTableName(options.table);
        if (client->DescribeTable(describe).IsSuccess()) return Status::Ok;

        Aws::DynamoDB::Model::CreateTableRequest create;
        create.SetTableName(options.table);
        create.AddAttributeDefinitions(Aws::DynamoDB::Model::AttributeDefinition()
                                           .WithAttributeName(attr::kPk)
                                           .WithAttributeType(
                                               Aws::DynamoDB::Model::ScalarAttributeType::S));
        create.AddAttributeDefinitions(Aws::DynamoDB::Model::AttributeDefinition()
                                           .WithAttributeName(attr::kSk)
                                           .WithAttributeType(
                                               Aws::DynamoDB::Model::ScalarAttributeType::S));
        create.AddKeySchema(Aws::DynamoDB::Model::KeySchemaElement()
                                .WithAttributeName(attr::kPk)
                                .WithKeyType(Aws::DynamoDB::Model::KeyType::HASH));
        create.AddKeySchema(Aws::DynamoDB::Model::KeySchemaElement()
                                .WithAttributeName(attr::kSk)
                                .WithKeyType(Aws::DynamoDB::Model::KeyType::RANGE));
        create.SetBillingMode(Aws::DynamoDB::Model::BillingMode::PAY_PER_REQUEST);
        if (!client->CreateTable(create).IsSuccess()) return Status::Io;

        for (int attempt = 0; attempt < 50; ++attempt) {
            auto described = client->DescribeTable(describe);
            if (described.IsSuccess() &&
                described.GetResult().GetTable().GetTableStatus() ==
                    Aws::DynamoDB::Model::TableStatus::ACTIVE) {
                return Status::Ok;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return Status::Io;
    }
};

DynamoManifestCatalog::DynamoManifestCatalog(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
DynamoManifestCatalog::~DynamoManifestCatalog() = default;

Result<std::shared_ptr<DynamoManifestCatalog>> DynamoManifestCatalog::open(DynamoOptions options) {
    if (options.table.empty() || options.store_id.empty()) return std::unexpected(Status::Config);

    auto impl = std::make_unique<Impl>();
    impl->options = options;

    Aws::Client::ClientConfiguration config;
    config.region = options.region;
    config.requestTimeoutMs = static_cast<long>(options.timeout.count());
    config.connectTimeoutMs = static_cast<long>(std::min<long long>(options.timeout.count(), 5'000));
    if (!options.endpoint.empty()) {
        config.endpointOverride = options.endpoint;
        config.scheme = options.endpoint.rfind("https://", 0) == 0 ? Aws::Http::Scheme::HTTPS
                                                                  : Aws::Http::Scheme::HTTP;
    }
    if (options.access_key.empty()) {
        impl->client = std::make_shared<Aws::DynamoDB::DynamoDBClient>(config);
    } else {
        impl->client = std::make_shared<Aws::DynamoDB::DynamoDBClient>(
            Aws::Auth::AWSCredentials(options.access_key, options.secret_key), config);
    }

    if (options.create_table_if_missing) {
        if (const Status status = impl->ensure_table(); status != Status::Ok) {
            return std::unexpected(status);
        }
    }
    return std::shared_ptr<DynamoManifestCatalog>(new DynamoManifestCatalog(std::move(impl)));
}

Result<std::optional<ManifestCatalog::Entry>> DynamoManifestCatalog::read() {
    Aws::DynamoDB::Model::GetItemRequest request;
    request.SetTableName(impl_->options.table);
    request.AddKey(attr::kPk, impl_->pk());
    request.AddKey(attr::kSk, Aws::DynamoDB::Model::AttributeValue(kPointerSk));
    // The pointer is the commit point; a stale read of it would let a fenced
    // writer believe it still owns the store.
    request.SetConsistentRead(true);

    auto outcome = impl_->client->GetItem(request);
    if (!outcome.IsSuccess()) return std::unexpected(Status::Io);

    const auto& item = outcome.GetResult().GetItem();
    // An absent item is an empty store, not a damaged one.
    if (item.empty()) return Result<std::optional<Entry>>(std::optional<Entry>{});

    const auto generation = item.find(attr::kGeneration);
    const auto version = item.find(attr::kVersion);
    if (generation == item.end() || version == item.end()) {
        return std::unexpected(Status::Corrupt);
    }

    uint64_t parsed = 0;
    const std::string& text = generation->second.GetN();
    if (std::from_chars(text.data(), text.data() + text.size(), parsed).ec != std::errc()) {
        return std::unexpected(Status::Corrupt);
    }
    return Result<std::optional<Entry>>(
        std::optional<Entry>(Entry{parsed, version->second.GetN()}));
}

Result<std::optional<ManifestCatalog::Entry>> DynamoManifestCatalog::compare_and_set(
    std::optional<Entry> expected, uint64_t generation) {
    uint64_t next_version = 1;
    if (expected.has_value()) {
        uint64_t current = 0;
        const std::string& token = expected->token;
        if (std::from_chars(token.data(), token.data() + token.size(), current).ec != std::errc()) {
            return std::unexpected(Status::Config);
        }
        next_version = current + 1;
    }

    Aws::DynamoDB::Model::UpdateItemRequest request;
    request.SetTableName(impl_->options.table);
    request.AddKey(attr::kPk, impl_->pk());
    request.AddKey(attr::kSk, Aws::DynamoDB::Model::AttributeValue(kPointerSk));
    request.SetUpdateExpression("SET #g = :g, #v = :v");
    request.AddExpressionAttributeNames("#g", attr::kGeneration);
    request.AddExpressionAttributeNames("#v", attr::kVersion);
    request.AddExpressionAttributeValues(
        ":g", Aws::DynamoDB::Model::AttributeValue().SetN(std::to_string(generation)));
    request.AddExpressionAttributeValues(
        ":v", Aws::DynamoDB::Model::AttributeValue().SetN(std::to_string(next_version)));

    if (expected.has_value()) {
        request.SetConditionExpression("#v = :expected");
        request.AddExpressionAttributeValues(
            ":expected", Aws::DynamoDB::Model::AttributeValue().SetN(expected->token));
    } else {
        // Nothing expected: the write must fail if a pointer appeared, or two
        // processes could both believe they installed the first generation.
        request.SetConditionExpression("attribute_not_exists(#v)");
    }

    auto outcome = impl_->client->UpdateItem(request);
    if (outcome.IsSuccess()) {
        return Result<std::optional<Entry>>(
            std::optional<Entry>(Entry{generation, std::to_string(next_version)}));
    }
    // **A failed condition is a lost CAS, not an error.** Another writer
    // installed first, so this process is fenced: nullopt, which the engine turns
    // into Status::Fenced and an instance that must be reopened. Anything else is
    // a genuine failure and stays retryable — conflating the two would either
    // fence a writer that never lost, or let a fenced one carry on.
    if (is_conditional_failure(outcome.GetError())) {
        return Result<std::optional<Entry>>(std::optional<Entry>{});
    }
    return std::unexpected(Status::Io);
}

std::future<Status> DynamoManifestCatalog::put_snapshot(uint64_t generation, Slice bytes) {
    return make_ready_future(impl_->put_chunked(
        [generation](uint32_t chunk) { return sort_key_snapshot(generation, chunk); }, bytes));
}

std::future<GetResult> DynamoManifestCatalog::get_snapshot(uint64_t generation) {
    return make_ready_future(impl_->get_chunked(snapshot_prefix(generation)));
}

/// **Chunked and compressed exactly as a snapshot is.** This wrote one raw item, so an edit had to
/// fit the 400 KB cap whole — and an edit carries a full `FileMetadata` per output file, five
/// strings each. The bound was `max_compaction_bytes / target_file_bytes x per-file record`, both
/// of which an embedder sets: lowering `target_file_bytes` to 256 KiB, which `options.hpp`
/// recommends for a hot tier, puts a default-sized compaction's edit at the cap, and modest keys
/// take it past. The store then cannot commit that compaction, ever.
std::future<Status> DynamoManifestCatalog::put_edit(uint64_t generation, uint64_t seq,
                                                   Slice bytes) {
    return make_ready_future(impl_->put_chunked(
        [generation, seq](uint32_t chunk) { return sort_key_edit(generation, seq, chunk); },
        bytes));
}

std::future<GetResult> DynamoManifestCatalog::get_edit(uint64_t generation, uint64_t seq) {
    return make_ready_future(impl_->get_chunked(edit_chunk_prefix(generation, seq)));
}

std::future<Result<std::vector<uint64_t>>> DynamoManifestCatalog::list_edits(uint64_t generation) {
    auto items = impl_->query_prefix(edit_prefix(generation));
    if (!items) {
        return make_ready_future(Result<std::vector<uint64_t>>(std::unexpected(items.error())));
    }

    // **One entry per edit, not per chunk.** A chunked edit is several items sharing a sequence
    // number, and the replay above this asks for each sequence once.
    std::set<uint64_t> seen;
    for (const auto& item : *items) {
        const std::string& sort_key = item.at(attr::kSk).GetS();
        // Read forward from the marker: `rfind('#')` finds the chunk index now, not the sequence.
        const size_t marker = sort_key.find(kEditMarker);
        if (marker == std::string::npos) continue;
        const char* begin = sort_key.data() + marker + kEditMarker.size();
        uint64_t seq = 0;
        // Stops at the separator before the chunk index, which is what makes this work for a key
        // whose sequence is not the last field.
        if (std::from_chars(begin, sort_key.data() + sort_key.size(), seq).ec == std::errc()) {
            seen.insert(seq);
        }
    }
    return make_ready_future(
        Result<std::vector<uint64_t>>(std::vector<uint64_t>(seen.begin(), seen.end())));
}

std::future<Status> DynamoManifestCatalog::delete_generation(uint64_t generation) {
    // Scoped to this generation's own sort-key prefix, so it cannot reach a
    // neighbour's items however the naming evolves — and never the pointer, whose
    // sort key does not carry the prefix.
    auto items = impl_->query_prefix(generation_prefix(generation));
    if (!items) return make_ready_future(items.error());
    if (items->empty()) return make_ready_future(Status::Ok);

    // BatchWriteItem takes at most 25 requests. Exceeding it is a validation
    // error, not a partial write, so this is a hard boundary.
    constexpr size_t kBatch = 25;
    for (size_t start = 0; start < items->size(); start += kBatch) {
        Aws::Vector<Aws::DynamoDB::Model::WriteRequest> writes;
        for (size_t i = start; i < std::min(start + kBatch, items->size()); ++i) {
            Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> key;
            key[attr::kPk] = impl_->pk();
            key[attr::kSk] = (*items)[i].at(attr::kSk);
            Aws::DynamoDB::Model::DeleteRequest remove;
            remove.SetKey(key);
            writes.push_back(Aws::DynamoDB::Model::WriteRequest().WithDeleteRequest(remove));
        }

        Aws::DynamoDB::Model::BatchWriteItemRequest request;
        request.AddRequestItems(impl_->options.table, writes);
        auto outcome = impl_->client->BatchWriteItem(request);
        if (!outcome.IsSuccess()) return make_ready_future(Status::Io);
        // Unprocessed items come back inside a 200 — throttling, not failure.
        // Reporting success while leaving items behind would stop the caller
        // retrying and strand them forever.
        if (!outcome.GetResult().GetUnprocessedItems().empty()) {
            return make_ready_future(Status::Io);
        }
    }
    return make_ready_future(Status::Ok);
}

}  // namespace elysiumkv
