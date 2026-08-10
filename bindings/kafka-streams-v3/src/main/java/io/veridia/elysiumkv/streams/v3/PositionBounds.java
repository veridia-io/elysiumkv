package io.veridia.elysiumkv.streams.v3;

import java.util.Map;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;

/**
 * Whether a store has advanced far enough to answer a bounded query.
 *
 * <p>Shared by every store here rather than reimplemented per store: the rule is subtle in a way
 * that fails silently. Getting it wrong does not throw — it serves older state than the caller
 * asked for, which is the failure a position bound exists to prevent.
 */
final class PositionBounds {
    private PositionBounds() {}

    /**
     * The rule Streams' own stores apply: a bound speaks only about <em>this</em> task's partition,
     * so a component naming another partition is not this store's business and is skipped. Within
     * our partition, never having seen the topic is a refusal — absence of evidence cannot be read
     * as being caught up.
     */
    static boolean isPermitted(Position reached, PositionBound bound, Integer partition) {
        if (bound.isUnbounded()) {
            return true;
        }
        if (partition == null) {
            // No context means no partition to compare against, so the demand cannot be shown to be
            // met. Refuse rather than assume.
            return false;
        }
        final Position required = bound.position();
        for (String topic : required.getTopics()) {
            final Map<Integer, Long> requiredOffsets = required.getPartitionPositions(topic);
            if (!requiredOffsets.containsKey(partition)) {
                continue;
            }
            final Long seen = reached.getPartitionPositions(topic).get(partition);
            if (seen == null || seen < requiredOffsets.get(partition)) {
                return false;
            }
        }
        return true;
    }
}
