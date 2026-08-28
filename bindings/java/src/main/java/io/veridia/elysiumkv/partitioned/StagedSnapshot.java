package io.veridia.elysiumkv.partitioned;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * What a staged set contributes to one scan, fixed when the scan starts.
 *
 * <p>Two questions, and between them the merge has no tie case: {@link #decides} says which
 * committed keys the staged set answers for — those are dropped from the scan — and
 * {@link #injections} delivers the staged puts in the scan's own order. A key can therefore arrive
 * from one side or the other but never from both.
 */
final class StagedSnapshot {

    static final StagedSnapshot EMPTY = new StagedSnapshot(
            new TreeMap<>(StagedOverlay.BY_KEY), new TreeSet<>(StagedOverlay.BY_KEY),
            Collections.emptyList(), false);

    private final NavigableMap<byte[], byte[]> puts;
    private final NavigableSet<byte[]> decided;
    private final List<StagedOverlay.Range> ranges;
    private final boolean reverse;

    StagedSnapshot(NavigableMap<byte[], byte[]> puts, NavigableSet<byte[]> decided,
                   List<StagedOverlay.Range> ranges, boolean reverse) {
        this.puts = puts;
        this.decided = decided;
        this.ranges = ranges;
        this.reverse = reverse;
    }

    /** Whether the staged set answers for this committed key, whether with a value or with absence. */
    boolean decides(byte[] key) {
        if (decided.contains(key)) {
            return true;
        }
        for (StagedOverlay.Range range : ranges) {
            if (range.covers(key)) {
                return true;
            }
        }
        return false;
    }

    /** The staged puts this scan must deliver, in its order. */
    Iterator<Map.Entry<byte[], byte[]>> injections() {
        return (reverse ? puts.descendingMap() : puts).entrySet().iterator();
    }
}
