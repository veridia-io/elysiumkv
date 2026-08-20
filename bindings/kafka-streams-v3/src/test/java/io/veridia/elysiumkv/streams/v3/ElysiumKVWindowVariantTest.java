package io.veridia.elysiumkv.streams.v3;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two window-store variants, and what happens when the wrong one is used.
 *
 * <p>The distinction is not cosmetic: Streams decides whether restored changelog values carry a
 * prepended timestamp by looking at the store's marker interface, while the live write path is
 * decided by the builder. Mismatch them and a store writes one format live and another on restore —
 * a divergence that appears only after a rebalance, which is the worst time to find it.
 */
class ElysiumKVWindowVariantTest {
    private static final String INPUT = "input";
    private static final String STORE = "windows";

    private static Topology processorTopology(WindowBytesStoreSupplier supplier) {
        Topology topology = new Topology();
        topology.addSource("src", Serdes.String().deserializer(), Serdes.Long().deserializer(),
                           INPUT)
                .addProcessor("proc", () -> new Processor<String, Long, Void, Void>() {
                    private WindowStore<String, Long> store;

                    @Override
                    public void init(ProcessorContext<Void, Void> context) {
                        store = context.getStateStore(STORE);
                    }

                    @Override
                    public void process(Record<String, Long> record) {
                        store.put(record.key(), record.value(), record.timestamp());
                    }
                }, "src")
                .addStateStore(Stores.windowStoreBuilder(supplier, Serdes.String(), Serdes.Long()),
                               "proc");
        return topology;
    }

    private static Properties config(Path stateDir) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "elysiumkv-window-variant-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        return props;
    }

    /** The plain variant in the position it is for: a Processor-API window store. */
    @Test
    void thePlainVariantBacksAProcessorApiStore(@TempDir Path dir) {
        Topology topology = processorTopology(ElysiumKVWindowBytesStoreSupplier.plain(
                STORE, ElysiumKVStoreConfig.local(), Duration.ofMinutes(10), Duration.ofSeconds(10),
                false));

        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(dir))) {
            driver.createInputTopic(INPUT, Serdes.String().serializer(),
                                    Serdes.Long().serializer())
                  .pipeInput("a", 7L);
            assertTrue(driver.getWindowStore(STORE) != null);
        }
    }

    /**
     * The mismatch is refused at startup, rather than left to diverge on the next restore.
     */
    @Test
    void theTimestampedVariantRefusesToBeAPlainStore(@TempDir Path dir) {
        Topology topology = processorTopology(ElysiumKVWindowBytesStoreSupplier.of(
                STORE, ElysiumKVStoreConfig.local(), Duration.ofMinutes(10), Duration.ofSeconds(10),
                false));

        Exception failure = assertThrows(Exception.class,
                                         () -> new TopologyTestDriver(topology, config(dir)));

        String message = rootCause(failure).getMessage();
        assertTrue(message != null && message.contains("plain"),
                   () -> "the failure should name the fix, but said: " + message);
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) cause = cause.getCause();
        return cause;
    }
}
