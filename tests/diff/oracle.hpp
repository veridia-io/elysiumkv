#ifndef ELYSIUMKV_TESTS_DIFF_ORACLE_HPP
#define ELYSIUMKV_TESTS_DIFF_ORACLE_HPP

#include <map>
#include <optional>
#include <string>
#include <vector>

namespace elysiumkv::test {

/// ARCHITECTURE.md "The differential oracle" — **`std::map<std::string, std::string>` under bytewise comparison is
/// the complete specification of observable behaviour.** With no sequence
/// numbers, snapshots, MVCC or column families, there is nothing else to model,
/// so this is a thin wrapper rather than a second storage engine.
class Oracle {
public:
    void put(const std::string& key, const std::string& value) { entries_[key] = value; }
    void remove(const std::string& key) { entries_.erase(key); }
    /// Erases everything below `key` — the whole of what `truncate_below` means, which is the
    /// point of stating it against a `std::map`.
    void truncate_below(const std::string& key) {
        entries_.erase(entries_.begin(), entries_.lower_bound(key));
    }

    /// Erases `[lower, upper)` — the whole of what `delete_range` means, which is the point of
    /// stating it against a `std::map`. An empty or inverted range erases nothing.
    void delete_range(const std::string& lower, const std::string& upper) {
        if (!(lower < upper)) return;
        entries_.erase(entries_.lower_bound(lower), entries_.lower_bound(upper));
    }

    std::optional<std::string> get(const std::string& key) const {
        auto it = entries_.find(key);
        if (it == entries_.end()) return std::nullopt;
        return it->second;
    }

    /// Entries in `[lower, upper)`; an empty `upper` means "to the end".
    std::vector<std::pair<std::string, std::string>> range(const std::string& lower,
                                                           const std::string& upper) const {
        std::vector<std::pair<std::string, std::string>> result;
        for (auto it = entries_.lower_bound(lower); it != entries_.end(); ++it) {
            if (!upper.empty() && it->first >= upper) break;
            result.push_back(*it);
        }
        return result;
    }

    std::vector<std::pair<std::string, std::string>> prefix(const std::string& prefix) const {
        std::vector<std::pair<std::string, std::string>> result;
        for (auto it = entries_.lower_bound(prefix); it != entries_.end(); ++it) {
            if (it->first.compare(0, prefix.size(), prefix) != 0) break;
            result.push_back(*it);
        }
        return result;
    }

    const std::map<std::string, std::string>& entries() const { return entries_; }
    size_t size() const { return entries_.size(); }

private:
    std::map<std::string, std::string> entries_;
};

}  // namespace elysiumkv::test

#endif  // ELYSIUMKV_TESTS_DIFF_ORACLE_HPP
