package io.veridia.elysiumkv;

import java.util.List;

/**
 * What {@link ElysiumKV#openWithResult} found. ARCHITECTURE.md "A tier is not a level" — a transient store that came
 * back empty or unreadable is discarded whole, and that is <b>not</b> the same
 * as missing recent writes — dropping a level's newer files uncovers older
 * values underneath, so reads afterwards can return stale data rather than no
 * data. {@link #requiresRecovery()} stays true until the embedder replays
 * whatever it needs and calls {@link ElysiumKV#markRecoveryComplete()}.
 */
public final class OpenResult {
    private final ElysiumKV db;
    private final List<String> discardedStores;
    private final long discardedFiles;
    private final boolean requiresRecovery;

    OpenResult(ElysiumKV db, List<String> discardedStores, long discardedFiles,
               boolean requiresRecovery) {
        this.db = db;
        // `List.copyOf`, not `unmodifiableList`. The latter is a *view*: it makes this reference
        // read-only while leaving the caller's list writable underneath it, so the safety depends on
        // the caller never keeping a reference — true today, and exactly the kind of thing that
        // stops being true later without anything here changing. Copying settles it locally, at the
        // cost of a handful of strings once per open.
        this.discardedStores = List.copyOf(discardedStores);
        this.discardedFiles = discardedFiles;
        this.requiresRecovery = requiresRecovery;
    }

    public ElysiumKV db() {
        return db;
    }

    /** Ids of the stores whose contents were dropped at open. */
    public List<String> discardedStores() {
        return discardedStores;
    }

    public long discardedFiles() {
        return discardedFiles;
    }

    /** True until {@link ElysiumKV#markRecoveryComplete()}. Reads may be stale. */
    public boolean requiresRecovery() {
        return requiresRecovery;
    }
}
