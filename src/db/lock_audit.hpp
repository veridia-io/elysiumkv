#ifndef ELYSIUMKV_DB_LOCK_AUDIT_HPP
#define ELYSIUMKV_DB_LOCK_AUDIT_HPP

namespace elysiumkv {

/// Counts engine mutexes held by this thread so `log_event` can assert it never hands control to an
/// embedder's sink under one — a blocking appender there stalls every writer, and a sink that
/// re-enters the store deadlocks. Compiled out entirely in release.
#ifndef NDEBUG

inline thread_local int g_locks_held = 0;

class LockAudit {
public:
    LockAudit() noexcept { ++g_locks_held; }
    ~LockAudit() noexcept { --g_locks_held; }
    LockAudit(const LockAudit&) = delete;
    LockAudit& operator=(const LockAudit&) = delete;
};

inline int locks_held() noexcept { return g_locks_held; }

#else

class LockAudit {};
inline int locks_held() noexcept { return 0; }

#endif

}  // namespace elysiumkv

/// Declare immediately after acquiring an engine mutex.
#define ELYSIUMKV_LOCK_AUDIT() [[maybe_unused]] ::elysiumkv::LockAudit elysiumkv_lock_audit_

#endif  // ELYSIUMKV_DB_LOCK_AUDIT_HPP
