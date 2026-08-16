package io.veridia.elysiumkv;

/**
 * What the native sink actually calls. A concrete final class rather than {@link ElysiumKVLogger}
 * directly, so the cached {@code jmethodID} resolves against a class instead of an interface — an
 * interface method id dispatched through {@code CallVoidMethod} is the kind of thing that works
 * until a JVM decides otherwise.
 *
 * <p>It also keeps the wire ints out of the public API: decoding happens here, once.
 */
final class LoggerBridge {

    private final ElysiumKVLogger delegate;

    LoggerBridge(ElysiumKVLogger delegate) {
        this.delegate = delegate;
    }

    /** Called from native, on an engine thread attached to the JVM for this purpose. */
    void log(int level, int event, String message) {
        delegate.log(ElysiumKVLogger.Level.of(level), ElysiumKVLogger.Event.of(event), message);
    }
}
