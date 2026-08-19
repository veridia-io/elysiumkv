package io.veridia.elysiumkv;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * One LocalStack container for the whole suite, plus the two preconditions the
 * remote tests need.
 *
 * <p><b>The image is pinned, and both obvious choices are wrong.</b> {@code latest}
 * refuses to start — "License activation failed". {@code 3.8} starts and
 * <em>silently ignores {@code If-Match}</em>: a stale ETag comes back 200 and
 * overwrites the object. That one is the dangerous one, because
 * {@code If-None-Match: *} does work there, so the write-once tests pass while
 * {@code compare_and_set} with a stale token succeeds — a suite showing the fence
 * working while two writers both install. 4.4.0 needs no licence and every
 * conditional is correct, which was verified before anything was built on it.
 *
 * <p><b>Skipping is deliberate but must not be silent.</b> A machine with no Docker,
 * or a native library built without {@code -DELYSIUMKV_BUILD_AWS=ON}, cannot run
 * these; they skip with a reason rather than fail. But a skip that nobody notices
 * is indistinguishable from a pass, so CI passes
 * {@code -Delysiumkv.remote.required=true} and the same missing precondition becomes
 * a failure there.
 */
public final class RemoteEnvironment {
    private static final String IMAGE = "localstack/localstack:4.4.0";
    private static final int PORT = 4566;

    /** Shared by every test; per-test isolation comes from prefixes and store ids. */
    public static final String BUCKET = "elysiumkv";
    public static final String TABLE = "elysiumkv-manifest";

    /** LocalStack accepts anything, and these are the conventional placeholders. */
    public static final String ACCESS_KEY = "test";
    public static final String SECRET_KEY = "test";

    private static GenericContainer<?> container;
    private static String endpoint;
    private static String kmsKeyId;

    private RemoteEnvironment() {}

    /**
     * Call first in every remote test. Either both preconditions hold and a
     * LocalStack endpoint is returned, or the test is skipped — or failed, when
     * {@code -Delysiumkv.remote.required=true} says this machine was supposed to be
     * able to run it.
     */
    public static synchronized String requireEndpoint() {
        boolean required = Boolean.getBoolean("elysiumkv.remote.required");

        if (!ElysiumKV.hasAwsSupport()) {
            String why = "the native library was built without ELYSIUMKV_BUILD_AWS, so there is no "
                    + "S3 or DynamoDB to test";
            if (required) fail(why);
            assumeTrue(false, why);
        }
        // Not isDockerAvailable(): it swallows the reason and answers a bare false,
        // so a daemon that is running but unusable — no permission, wrong socket,
        // an image it cannot fetch — is indistinguishable from no Docker at all.
        // The whole point of gating rather than failing is that the reason reaches
        // whoever reads the report.
        try {
            DockerClientFactory.instance().client();
        } catch (Throwable unusable) {
            String why = "Docker is not usable, so LocalStack cannot be started: " + unusable;
            if (required) fail(why);
            assumeTrue(false, why);
        }

        if (endpoint == null) {
            container = new GenericContainer<>(IMAGE)
                                .withExposedPorts(PORT)
                                .withEnv("SERVICES", "s3,dynamodb,kms")
                                // The health endpoint, not a log line: a log
                                // message can appear before the service is
                                // actually answering, and the first failure would
                                // then look like a bug in the store.
                                .waitingFor(Wait.forHttp("/_localstack/health")
                                                    .forPort(PORT)
                                                    .forStatusCode(200)
                                                    .withStartupTimeout(Duration.ofMinutes(3)));
            container.start();
            endpoint = "http://" + container.getHost() + ":" + container.getMappedPort(PORT);
            createBucket();
        }
        return endpoint;
    }

    /**
     * A KMS key for the encryption tests, made on first use.
     *
     * <p>LocalStack starts with none, and a key is cheap enough that one shared across the suite is
     * fine: what the tests vary is the store, not the key. Call {@link #requireEndpoint()} first —
     * this assumes the container is up.
     */
    public static synchronized String requireKmsKeyId() {
        requireEndpoint();
        if (kmsKeyId == null) {
            kmsKeyId = exec("creating a KMS key", "awslocal", "kms", "create-key", "--query",
                            "KeyMetadata.KeyId", "--output", "text")
                               .trim();
            if (kmsKeyId.isEmpty()) throw new IllegalStateException("KMS returned no key id");
        }
        return kmsKeyId;
    }

    /** Runs a CLI command in the container, failing loudly rather than returning a bad value. */
    private static String exec(String what, String... command) {
        try {
            GenericContainer.ExecResult result = container.execInContainer(command);
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(
                        what + " failed: " + result.getStdout() + result.getStderr());
            }
            return result.getStdout();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(what + " failed", e);
        }
    }

    /**
     * {@code S3BlobStore} has no create-bucket option on purpose — a bucket is
     * provisioned infrastructure, and creating one silently would hide a
     * misconfigured name behind a working store. So the fixture provisions it, with
     * the CLI the image already ships rather than by adding an AWS SDK to this
     * classpath. The DynamoDB table needs no equivalent: {@code
     * createTableIfMissing} is a real option there, and letting the tests use it
     * exercises that path too.
     */
    private static void createBucket() {
        try {
            GenericContainer.ExecResult result =
                    container.execInContainer("awslocal", "s3", "mb", "s3://" + BUCKET);
            // "BucketAlreadyOwnedByYou" is success for this purpose; anything else
            // is not, and a bucket that quietly failed to appear would surface
            // later as a pile of unexplained Io failures.
            if (result.getExitCode() != 0 && !result.getStderr().contains("BucketAlreadyOwnedByYou")) {
                throw new IllegalStateException("creating s3://" + BUCKET + " failed: "
                        + result.getStdout() + result.getStderr());
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("creating s3://" + BUCKET + " failed", e);
        }
    }
}
