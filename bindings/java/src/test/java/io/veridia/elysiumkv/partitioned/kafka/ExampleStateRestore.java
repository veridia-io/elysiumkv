package io.veridia.elysiumkv.partitioned.kafka;

import io.veridia.elysiumkv.partitioned.Mutation;
import io.veridia.elysiumkv.partitioned.Restore;
import io.veridia.elysiumkv.partitioned.WriteSink;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Replays a state topic into a partition — a worked example, not shipped API.
 *
 * <p>It lives in test sources deliberately. {@code PartitionedStore} does not own the caller's
 * consumer, its configuration or its lifecycle, and this class does all three: it takes a factory,
 * runs a poll loop and closes what it was given. A real replay wants things this does not have —
 * cancellation, metrics, backpressure, and pause/resume on the main consumer — so shipping it would
 * be offering an abstraction the first serious caller has to replace.
 *
 * <p>What is worth keeping is the arithmetic, which is what the tests beside it pin. Copy it.
 *
 * <p>Three things here are load-bearing:
 *
 * <ul>
 *   <li>The seek is {@code materializedThrough + 1}. The parameter is inclusive — it is the
 *       last position the store already holds — so resuming at it would replay one record twice.
 *       Harmless here because a replayed put is idempotent, but the same off-by-one in the other
 *       direction silently skips a record, and the name is what keeps the two apart.
 *   <li>The consumer must be {@code read_committed}, which makes {@code endOffsets} the last
 *       <em>stable</em> offset. Under {@code read_uncommitted} a replay restores records from an open
 *       or aborted transaction as though they were state.
 *   <li>The position travels with each batch. Reporting it only at the end would leave a
 *       restore that dies partway with nothing to show for it, and a cold restore reporting nothing
 *       at all until the first later commit.
 * </ul>
 *
 * @param <K> the store's key type
 */
final class ExampleStateRestore<K> implements Restore<K> {

    private final IntFunction<Consumer<byte[], byte[]>> consumers;
    private final String topic;
    private final Function<byte[], K> keyFrom;
    private final MutationCodec codec;
    private final Duration pollTimeout;
    private final int batchSize;

    ExampleStateRestore(IntFunction<Consumer<byte[], byte[]>> consumers, String topic,
                             Function<byte[], K> keyFrom, MutationCodec codec,
                             Duration pollTimeout, int batchSize) {
        this.consumers = Objects.requireNonNull(consumers, "consumers");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.keyFrom = Objects.requireNonNull(keyFrom, "keyFrom");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout");
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        this.batchSize = batchSize;
    }

    @Override
    public void restore(int partition, OptionalLong materializedThrough, WriteSink<K> sink) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        try (Consumer<byte[], byte[]> consumer = consumers.apply(partition)) {
            consumer.assign(Collections.singletonList(topicPartition));
            if (materializedThrough.isPresent()) {
                consumer.seek(topicPartition, materializedThrough.getAsLong() + 1);
            } else {
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
            }

            // read_committed, so this is the last stable offset rather than the log end.
            long end = consumer.endOffsets(Collections.singletonList(topicPartition))
                    .get(topicPartition);

            Map<K, Mutation> batch = new LinkedHashMap<>();
            long through = -1L;
            while (consumer.position(topicPartition) < end) {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(pollTimeout);
                for (ConsumerRecord<byte[], byte[]> record : polled.records(topicPartition)) {
                    if (record.offset() >= end) {
                        break;
                    }
                    if (ChangelogRecords.isRange(record.key())) {
                        // Flushed first: a range delete covers what exists at its own position, so
                        // everything below it has to be applied before it.
                        if (through >= 0) {
                            sink.putBatch(through, batch);
                            batch = new LinkedHashMap<>();
                            through = -1L;
                        }
                        sink.deleteRange(record.offset(), ChangelogRecords.lowerBound(record.key()),
                                ChangelogRecords.upperBound(record.key()));
                        continue;
                    }
                    batch.put(keyFrom.apply(ChangelogRecords.entityKey(record.key())),
                            codec.decode(record.value()));
                    through = record.offset();
                    if (batch.size() >= batchSize) {
                        sink.putBatch(through, batch);
                        batch = new LinkedHashMap<>();
                        through = -1L;
                    }
                }
            }
            if (through >= 0) {
                sink.putBatch(through, batch);
            }
        }
    }
}
