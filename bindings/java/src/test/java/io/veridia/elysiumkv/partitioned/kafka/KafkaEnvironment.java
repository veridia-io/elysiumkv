package io.veridia.elysiumkv.partitioned.kafka;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;

/**
 * One broker for the whole suite, and the topics the tests need.
 *
 * <p><b>Why a real broker at all</b>, when the differential suite already drives the protocol
 * against {@code InMemoryLog}: the thing a fake log cannot supply is Kafka's own semantics. A
 * transaction that aborts leaves records physically present in the partition that a
 * {@code read_committed} consumer must not see; the last stable offset is not the log end;
 * offsets are assigned by the broker rather than by a counter the test controls; and a producer
 * is fenced by another producer taking its {@code transactional.id}, not by a flag someone sets.
 * Those are the paths where {@code PartitionedStore}'s outcome handling either matches reality or
 * does not.
 *
 * <p>The image tracks {@code kafka.version} in the pom. KRaft, so no ZooKeeper, and the
 * transaction-state topic is forced to a single replica because a one-broker cluster cannot
 * satisfy the default of three — without it every {@code initTransactions()} hangs until it times
 * out, which reads as "transactions are broken" rather than "this cluster is too small".
 *
 * <p>Skipping is deliberate but must not be silent — same rule as {@code RemoteEnvironment}: no
 * Docker means skip with a reason, unless {@code -Delysiumkv.kafka.required=true} says this
 * machine was supposed to be able to run it.
 */
final class KafkaEnvironment {
    private static final String IMAGE = "apache/kafka:3.9.2";

    private static KafkaContainer container;
    private static String bootstrap;

    private KafkaEnvironment() {}

    static synchronized String requireBootstrap() {
        boolean required = Boolean.getBoolean("elysiumkv.kafka.required");

        // Not isDockerAvailable(): it answers a bare false and swallows why, so an unusable daemon
        // is indistinguishable from an absent one. See RemoteEnvironment.
        try {
            DockerClientFactory.instance().client();
        } catch (Throwable unusable) {
            String why = "Docker is not usable, so no broker can be started: " + unusable;
            if (required) fail(why);
            assumeTrue(false, why);
        }

        if (bootstrap == null) {
            container = new KafkaContainer(IMAGE)
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                    .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                    .withStartupTimeout(Duration.ofMinutes(3));
            container.start();
            bootstrap = container.getBootstrapServers();
        }
        return bootstrap;
    }

    /**
     * Freezes the broker's processes without tearing the connection down, which is what makes a
     * commit <em>indeterminate</em> rather than refused: the client's request is outstanding and no
     * answer is coming. Killing the container instead resets the connection, and the producer
     * learns the commit definitely did not happen — the opposite of the case under test.
     */
    static void pauseBroker() {
        requireBootstrap();
        DockerClientFactory.instance().client()
                .pauseContainerCmd(container.getContainerId()).exec();
    }

    static void resumeBroker() {
        DockerClientFactory.instance().client()
                .unpauseContainerCmd(container.getContainerId()).exec();
    }

    /**
     * Created explicitly rather than by auto-creation, which would give one partition and make
     * every partitioned test silently degenerate to the single-partition case.
     */
    static void createTopic(String name, int partitions) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, requireBootstrap());
        try (Admin admin = Admin.create(config)) {
            admin.createTopics(Collections.singletonList(new NewTopic(name, partitions, (short) 1)))
                    .all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new IllegalStateException("creating topic " + name + " failed", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted creating topic " + name, e);
        }
    }
}
