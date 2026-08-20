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


#include <algorithm>
#include <mutex>
#include <random>
#include <set>
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
/// The instance that wrote a chunk. Not in the sort key, deliberately: the address has to stay
/// write-once, because a collision there is the only thing that detects a second writer between
/// generation rolls. This attribute only answers "is the thing already at that address *mine*",
/// which is what makes a retry over one's own residue safe and a retry over a rival's a fence.
constexpr const char* kWriter = "writer";
/// The snapshot attempt the pointer install committed to. See `sort_key_snapshot`.
constexpr const char* kSnapshotAttempt = "snapshot_attempt";
}  // namespace attr

constexpr const char* kPointerSk = "POINTER";

/// An item caps at 400 KB including attribute names and keys, so the payload
/// budget has to sit well below it. 300 KB leaves room for the key, the sort key
/// and the bookkeeping attributes without arithmetic that has to be revisited
/// whenever one is added.
constexpr size_t kChunkPayloadBytes = 300 * 1024;

/// A snapshot address carries the attempt that wrote it, which is what makes the write idempotent:
/// chunks go in one at a time, so a failure partway leaves residue, and a fixed address would make
/// the retry collide with it. An unfinished attempt is an incomplete set no reader selects, and
/// `delete_generation` collects it with the rest of the generation.
///
/// Which attempt is authoritative is decided by the pointer install, not here: two writers rolling
/// to one generation both write complete snapshots, and the CAS winner records its attempt on the
/// pointer. Without that record the generation is unambiguous only while one complete set exists.
std::string sort_key_snapshot(uint64_t generation, const std::string& attempt, uint32_t chunk) {
    char buf[96];
    std::snprintf(buf, sizeof(buf), "gen#%012llu#snap#%s#%04u",
                  static_cast<unsigned long long>(generation), attempt.c_str(), chunk);
    return buf;
}

/// The attempt id inside a snapshot sort key, or empty if it is not one of ours.
std::string attempt_of(const std::string& sort_key, const std::string& prefix) {
    if (sort_key.size() <= prefix.size()) return {};
    const size_t end = sort_key.find('#', prefix.size());
    if (end == std::string::npos) return {};
    return sort_key.substr(prefix.size(), end - prefix.size());
}

/// Sixteen hex characters of randomness. Fixed width, so a sort key stays parseable by position,
/// and wide enough that two attempts colliding is not a case anyone has to reason about.
std::string fresh_id() {
    static std::mutex mutex;
    static std::mt19937_64 rng(std::random_device{}());
    uint64_t value = 0;
    {
        std::lock_guard<std::mutex> lock(mutex);
        value = rng();
    }
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(value));
    return buf;
}

/// The chunk index is new, so an edit written by an earlier build is unreadable here. That
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

    /// This instance, for as long as it lives. Written onto every chunk so a retry can tell its
    /// own residue from a rival's in-flight write — see `attr::kWriter`.
    const std::string writer = fresh_id();

    mutable std::mutex attempts_mutex;
    /// generation -> the attempt this instance last wrote a snapshot under, so the pointer install
    /// that follows can record which one it is committing to.
    std::map<uint64_t, std::string> written_attempts;
    /// generation -> the attempt the pointer names, learned by `read()`. A reader gets this from
    /// the winner's install rather than by guessing.
    std::map<uint64_t, std::string> pointer_attempts;

    void note_written(uint64_t generation, const std::string& attempt) {
        std::lock_guard<std::mutex> lock(attempts_mutex);
        written_attempts[generation] = attempt;
    }
    std::string written_attempt(uint64_t generation) const {
        std::lock_guard<std::mutex> lock(attempts_mutex);
        const auto found = written_attempts.find(generation);
        return found == written_attempts.end() ? std::string() : found->second;
    }
    void note_pointer(uint64_t generation, const std::string& attempt) {
        std::lock_guard<std::mutex> lock(attempts_mutex);
        if (!attempt.empty()) pointer_attempts[generation] = attempt;
    }
    std::string pointer_attempt(uint64_t generation) const {
        std::lock_guard<std::mutex> lock(attempts_mutex);
        const auto found = pointer_attempts.find(generation);
        return found == pointer_attempts.end() ? std::string() : found->second;
    }

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
        request.AddItem(attr::kWriter, Aws::DynamoDB::Model::AttributeValue(writer));
        if (total_chunks.has_value()) {
            request.AddItem(attr::kTotalChunks, Aws::DynamoDB::Model::AttributeValue().SetN(
                                                    std::to_string(*total_chunks)));
        }
        request.SetConditionExpression("attribute_not_exists(SK)");

        auto outcome = client->PutItem(request);
        if (outcome.IsSuccess()) return Status::Ok;
        if (is_conditional_failure(outcome.GetError())) return Status::Config;
        // Terminal, not retryable. A rejected *request* — an item over the 400 KB cap, a table
        // whose schema does not match — will be rejected identically forever, and `Status::Io`
        // means "ask again later" to everything above. That turns a permanent failure into a
        // background retry loop whose only symptom is `background_failures` climbing.
        if (is_validation_failure(outcome.GetError())) return Status::Config;
        return Status::Io;
    }

    /// Splits and writes `bytes` as chunks addressed by `sort_key(index)`.
    ///
    /// Chunking only: the engine compresses above the seal, where it still sees plaintext.
    /// Chunking belongs here because the 400 KB item cap is DynamoDB's alone.
    ///
    /// Chunked unconditionally rather than above a threshold, so the path is not one that only
    /// runs in production.
    Status put_chunked(const std::function<std::string(uint32_t)>& sort_key, Slice bytes) {
        const std::string packed(reinterpret_cast<const char*>(bytes.data()), bytes.size());

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

    /// The snapshot for `generation`, choosing between attempts.
    ///
    /// The pointer decides when it can. Two writers rolling to one generation both write
    /// complete snapshots, and only the compare-and-set says which of them owns the store; the
    /// winner records its attempt as it installs, and `read()` picks that up. Falling back to "the
    /// only complete set" is correct exactly while there is one — with several and no pointer to
    /// arbitrate, guessing would be choosing a version of the file list at random, so it refuses.
    GetResult get_snapshot_attempt(uint64_t generation) {
        const std::string prefix = snapshot_prefix(generation);
        auto items = query_prefix(prefix);
        if (!items) return std::unexpected(items.error());
        if (items->empty()) return std::unexpected(Status::NotFound);

        // Grouped in memory from the one query the prefix already answered: an attempt that never
        // finished is present but short, and is exactly what must not be selected.
        std::map<std::string, std::vector<const Aws::Map<Aws::String,
                                                         Aws::DynamoDB::Model::AttributeValue>*>>
            by_attempt;
        for (const auto& item : *items) {
            std::string attempt = attempt_of(item.at(attr::kSk).GetS(), prefix);
            if (!attempt.empty()) by_attempt[std::move(attempt)].push_back(&item);
        }

        std::vector<std::string> complete;
        for (const auto& [attempt, chunks] : by_attempt) {
            const auto total = chunks.front()->find(attr::kTotalChunks);
            if (total == chunks.front()->end()) continue;
            uint32_t declared = 0;
            const std::string& text = total->second.GetN();
            std::from_chars(text.data(), text.data() + text.size(), declared);
            if (declared != 0 && declared == chunks.size()) complete.push_back(attempt);
        }

        const std::string named = pointer_attempt(generation);
        if (!named.empty()) {
            if (std::find(complete.begin(), complete.end(), named) == complete.end()) {
                // The pointer names an attempt that is absent or short. That is a damaged store,
                // not a choice to be made — picking a different complete attempt would serve a
                // version of the file list the install never committed to.
                return std::unexpected(Status::Corrupt);
            }
            return get_chunked(prefix + named + "#");
        }
        // No record of which attempt was installed — a pointer written before a snapshot existed
        // for this generation. Unambiguous exactly while one attempt finished.
        if (complete.size() == 1) return get_chunked(prefix + complete.front() + "#");
        if (complete.empty()) return std::unexpected(Status::Corrupt);
        return std::unexpected(Status::Corrupt);
    }

    /// Every item under `prefix`, removed. Scoped by prefix so it cannot reach a neighbour's items
    /// however the naming evolves — and never the pointer, whose sort key carries no prefix.
    Status delete_prefix(const std::string& prefix) {
        auto items = query_prefix(prefix);
        if (!items) return items.error();
        if (items->empty()) return Status::Ok;

        // BatchWriteItem takes at most 25 requests. Exceeding it is a validation error, not a
        // partial write, so this is a hard boundary.
        constexpr size_t kBatch = 25;
        for (size_t start = 0; start < items->size(); start += kBatch) {
            Aws::Vector<Aws::DynamoDB::Model::WriteRequest> writes;
            for (size_t i = start; i < std::min(start + kBatch, items->size()); ++i) {
                Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> key;
                key[attr::kPk] = pk();
                key[attr::kSk] = (*items)[i].at(attr::kSk);
                Aws::DynamoDB::Model::DeleteRequest remove;
                remove.SetKey(key);
                writes.push_back(Aws::DynamoDB::Model::WriteRequest().WithDeleteRequest(remove));
            }

            Aws::DynamoDB::Model::BatchWriteItemRequest request;
            request.AddRequestItems(options.table, writes);
            auto outcome = client->BatchWriteItem(request);
            if (!outcome.IsSuccess()) return Status::Io;
            // Unprocessed items come back inside a 200 — throttling, not failure. Reporting success
            // while leaving items behind would stop the caller retrying and strand them forever.
            if (!outcome.GetResult().GetUnprocessedItems().empty()) return Status::Io;
        }
        return Status::Ok;
    }

    /// Whether any attempt at `generation` finished. What write-once is about for a snapshot: an
    /// unfinished one is residue and may be retried past, a finished one is the snapshot.
    Result<bool> has_complete_snapshot(uint64_t generation) {
        const std::string prefix = snapshot_prefix(generation);
        auto items = query_prefix(prefix);
        if (!items) return std::unexpected(items.error());
        if (items->empty()) return false;

        std::map<std::string, size_t> counts;
        std::map<std::string, uint32_t> declared;
        for (const auto& item : *items) {
            std::string attempt = attempt_of(item.at(attr::kSk).GetS(), prefix);
            if (attempt.empty()) continue;
            ++counts[attempt];
            const auto total = item.find(attr::kTotalChunks);
            if (total == item.end()) continue;
            uint32_t value = 0;
            const std::string& text = total->second.GetN();
            std::from_chars(text.data(), text.data() + text.size(), value);
            declared[attempt] = value;
        }
        for (const auto& [attempt, count] : counts) {
            const auto total = declared.find(attempt);
            if (total != declared.end() && total->second != 0 && total->second == count) return true;
        }
        return false;
    }

    /// Whether the chunk set under `prefix` was never finished: present, but fewer items than the
    /// count every chunk carries. Empty is not partial — there is nothing there to clear.
    Result<bool> is_partial(const std::string& prefix) {
        auto items = query_prefix(prefix);
        if (!items) return std::unexpected(items.error());
        if (items->empty()) return false;

        const auto total = items->front().find(attr::kTotalChunks);
        if (total == items->front().end()) return false;
        uint32_t declared = 0;
        const std::string& text = total->second.GetN();
        std::from_chars(text.data(), text.data() + text.size(), declared);
        return declared != 0 && declared != items->size();
    }

    /// Whether the item at `sort_key` was written by this instance, so a retry may clear it.
    ///
    /// Absent counts as ours. The only reason to ask is that a write just failed the
    /// write-once condition, so something is there; a race that removes it between the two calls
    /// leaves the address free, which is the same position as finding our own residue.
    Result<bool> written_by_us(const std::string& sort_key) {
        Aws::DynamoDB::Model::GetItemRequest request;
        request.SetTableName(options.table);
        request.AddKey(attr::kPk, pk());
        request.AddKey(attr::kSk, Aws::DynamoDB::Model::AttributeValue(sort_key));
        request.SetConsistentRead(true);
        auto outcome = client->GetItem(request);
        if (!outcome.IsSuccess()) return std::unexpected(Status::Io);

        const auto& item = outcome.GetResult().GetItem();
        if (item.empty()) return true;
        const auto found = item.find(attr::kWriter);
        // An item with no writer attribute predates this and cannot be claimed.
        if (found == item.end()) return false;
        return found->second.GetS() == writer;
    }

    /// One edit address, written once, retried only over residue this instance left.
    ///
    /// The address stays `(generation, seq)` rather than carrying an attempt like a snapshot does,
    /// because its write-once-ness is the only thing that detects a second writer between
    /// generation rolls: open takes no lock and performs no compare-and-set, so two writers both
    /// believe they own the store until one collides here. Per-attempt addresses would let both
    /// succeed and leave replay to pick one silently.
    ///
    /// So the writer attribute does the work instead: an unfinished set this instance wrote is
    /// cleared and the write retried, and anything else stays `Config`, which the engine turns into
    /// a fence. A crash is not covered — the residue then carries a dead instance's id,
    /// indistinguishable from a live rival's — so the store fences until an operator removes it.
    Status put_edit_chunked(uint64_t generation, uint64_t seq, Slice bytes) {
        const auto address = [generation, seq](uint32_t chunk) {
            return sort_key_edit(generation, seq, chunk);
        };
        const Status first = put_chunked(address, bytes);
        if (first != Status::Config) return first;

        // Residue means *incomplete*. A complete set at this address is a real edit, and
        // overwriting it would break the write-once contract the whole ownership protocol rests on
        // — `next_seq_` is monotonic, so this instance reaching an address it already filled is a
        // programming error and must stay one. Only a set that was never finished can be cleared.
        auto partial = is_partial(edit_chunk_prefix(generation, seq));
        if (!partial) return partial.error();
        if (!*partial) return Status::Config;

        auto ours = written_by_us(address(0));
        if (!ours) return ours.error();
        if (!*ours) return Status::Config;

        if (const Status cleared = delete_prefix(edit_chunk_prefix(generation, seq));
            cleared != Status::Ok) {
            return cleared;
        }
        return put_chunked(address, bytes);
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

        Buffer packed;
        for (const auto& item : *items) {
            const auto payload = item.find(attr::kPayload);
            if (payload == item.end()) return std::unexpected(Status::Corrupt);
            const auto& bytes = payload->second.GetB();
            packed.insert(packed.end(), bytes.GetUnderlyingData(),
                          bytes.GetUnderlyingData() + bytes.GetLength());
        }
        return packed;
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
    // The attempt the winning install committed to, so a reader that wrote none of this still
    // reads the right snapshot. Absent on a pointer written before a snapshot existed for the
    // generation — `create()` installs generation 1 with nothing under it — which the read below
    // handles by falling back to the only complete set.
    const auto attempt = item.find(attr::kSnapshotAttempt);
    if (attempt != item.end()) impl_->note_pointer(parsed, attempt->second.GetS());

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
    // The install is what makes one snapshot attempt authoritative, so it records which one.
    // Written in the same conditional update as the generation and the version: a pointer naming a
    // generation without naming the attempt it committed to would leave a reader guessing between
    // two complete snapshots, which is the case this whole layout exists to make decidable.
    const std::string attempt = impl_->written_attempt(generation);
    request.SetUpdateExpression(attempt.empty() ? "SET #g = :g, #v = :v"
                                                : "SET #g = :g, #v = :v, #a = :a");
    request.AddExpressionAttributeNames("#g", attr::kGeneration);
    request.AddExpressionAttributeNames("#v", attr::kVersion);
    request.AddExpressionAttributeValues(
        ":g", Aws::DynamoDB::Model::AttributeValue().SetN(std::to_string(generation)));
    request.AddExpressionAttributeValues(
        ":v", Aws::DynamoDB::Model::AttributeValue().SetN(std::to_string(next_version)));
    if (!attempt.empty()) {
        request.AddExpressionAttributeNames("#a", attr::kSnapshotAttempt);
        request.AddExpressionAttributeValues(":a", Aws::DynamoDB::Model::AttributeValue(attempt));
    }

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
        impl_->note_pointer(generation, attempt);
        return Result<std::optional<Entry>>(
            std::optional<Entry>(Entry{generation, std::to_string(next_version)}));
    }
    // A failed condition is a lost CAS, not an error. Another writer
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
    // Write-once is a contract every catalog keeps, so a *complete* snapshot still refuses.
    // The attempt in the address exists to make a retry over an unfinished one possible, not to
    // make a generation's snapshot rewritable — `ManifestCatalogContract.ObjectsAreWriteOnce`
    // holds the disk, S3 and DynamoDB implementations to the same answer, and one of them quietly
    // becoming rewritable is the kind of divergence that suite exists to catch.
    //
    // Racy by construction, and deliberately not the ownership mechanism: the compare-and-set on
    // the pointer is, and it follows immediately. This is the write-once *contract*, not the fence.
    if (auto existing = impl_->has_complete_snapshot(generation); !existing) {
        return make_ready_future(existing.error());
    } else if (*existing) {
        return make_ready_future(Status::Config);
    }

    // A fresh attempt per call, so this never collides with its own residue and a retry after a
    // failure partway through is simply another attempt.
    const std::string attempt = fresh_id();
    const Status status = impl_->put_chunked(
        [generation, &attempt](uint32_t chunk) {
            return sort_key_snapshot(generation, attempt, chunk);
        },
        bytes);
    // Remembered even on failure: harmless, and the pointer install only ever names an attempt it
    // could reach, because a failed `put_snapshot` stops the roll before the CAS.
    if (status == Status::Ok) impl_->note_written(generation, attempt);
    return make_ready_future(status);
}

std::future<GetResult> DynamoManifestCatalog::get_snapshot(uint64_t generation) {
    return make_ready_future(impl_->get_snapshot_attempt(generation));
}

/// Chunked exactly as a snapshot is. This wrote one raw item, so an edit had to
/// fit the 400 KB cap whole — and an edit carries a full `FileMetadata` per output file, five
/// strings each. The bound was `max_compaction_bytes / target_file_bytes x per-file record`, both
/// of which an embedder sets: lowering `target_file_bytes` to 256 KiB, which `options.hpp`
/// recommends for a hot tier, puts a default-sized compaction's edit at the cap, and modest keys
/// take it past. The store then cannot commit that compaction, ever.
std::future<Status> DynamoManifestCatalog::put_edit(uint64_t generation, uint64_t seq,
                                                    Slice bytes) {
    return make_ready_future(impl_->put_edit_chunked(generation, seq, bytes));
}

std::future<GetResult> DynamoManifestCatalog::get_edit(uint64_t generation, uint64_t seq) {
    return make_ready_future(impl_->get_chunked(edit_chunk_prefix(generation, seq)));
}

std::future<Result<std::vector<uint64_t>>> DynamoManifestCatalog::list_edits(uint64_t generation) {
    auto items = impl_->query_prefix(edit_prefix(generation));
    if (!items) {
        return make_ready_future(Result<std::vector<uint64_t>>(std::unexpected(items.error())));
    }

    // One entry per edit, not per chunk. A chunked edit is several items sharing a sequence
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
    // This is also what collects a failed snapshot attempt. A partial attempt leaves chunks
    // under `gen#G#snap#<attempt>#…`; the retry installs the generation under a *different*
    // attempt, and when the generation after that is installed this removes the whole prefix —
    // the good snapshot, the dead attempt and the edits together. So the residue is bounded by one
    // generation's lifetime and needs no sweep of its own.
    return make_ready_future(impl_->delete_prefix(generation_prefix(generation)));
}

}  // namespace elysiumkv
