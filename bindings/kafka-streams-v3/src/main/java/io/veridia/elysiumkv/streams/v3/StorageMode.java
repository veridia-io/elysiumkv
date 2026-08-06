package io.veridia.elysiumkv.streams.v3;

/**
 * Where a store's bytes live. The engine is the same in all three; only the tier configuration
 * differs, and the choice is a point on one curve rather than a quality ranking.
 *
 * <table>
 *   <caption>The trade</caption>
 *   <tr><th></th><th>Object-store requests</th><th>Restore after pod loss</th></tr>
 *   <tr><td>{@link #LOCAL}</td><td>none</td><td>full changelog replay</td></tr>
 *   <tr><td>{@link #REMOTE}</td><td>every flush, every cold read</td><td>near zero</td></tr>
 *   <tr><td>{@link #HYBRID}</td><td>only migrated files</td><td>the tail since the last migration</td></tr>
 * </table>
 *
 * <p><b>The advantage is not being cheaper than both ends. It is that the trade is a dial</b> — the
 * hot tier's {@code maxAge} sets it, so an adopter computes their own answer rather than trusting
 * someone else's benchmark.
 */
public enum StorageMode {
    /**
     * One durable local tier. Comparable to RocksDB, and honestly costed: there is no write-ahead
     * log, so a commit becomes an SST — index, bloom filter, footer and the compaction debt that
     * follows — rather than a sequential append. Higher per-commit cost and a much smaller feature
     * surface.
     */
    LOCAL,

    /**
     * One durable object-store tier, normally behind a disk cache. Every flush is a PUT and every
     * cold read a GET, so a cache chain is not optional in practice.
     */
    REMOTE,

    /**
     * A transient local tier over a durable object-store tier — the configuration this engine exists
     * for, and the only one where local disk may be ephemeral.
     *
     * <p><b>Not available in Phase 1, and rejected rather than quietly degraded.</b> It needs
     * store-managed changelog offsets (KIP-1035, Kafka 4.x): without them Streams' checkpoint file
     * assumes local state is durable, and after a pod loss it would replay from an offset whose
     * state lived only on the tier that vanished. The store cannot correct that from inside, so the
     * mode is refused at construction instead of failing later as missing data.
     */
    HYBRID
}
