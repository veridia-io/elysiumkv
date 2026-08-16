package io.veridia.elysiumkv;

/**
 * Receives what the engine has to say about itself. The engine keeps no log of its own, so this is
 * the only channel: without one, a background failure that is retried leaves no trace anywhere.
 *
 * <p><b>Called on engine threads — flush, compaction and maintenance — synchronously.</b> The
 * calling thread is the one that produced the event and it is waiting, so a slow implementation
 * applies backpressure to that operation. Hand the line to an async appender rather than doing work
 * here. SLF4J's async appenders are the intended shape:
 *
 * <pre>{@code
 * options.logger((level, event, message) -> {
 *     switch (level) {
 *         case ERROR: LOG.error("elysiumkv {}: {}", event, message); break;
 *         case WARN:  LOG.warn("elysiumkv {}: {}", event, message);  break;
 *         default:    LOG.info("elysiumkv {}: {}", event, message);  break;
 *     }
 * }, ElysiumKVLogger.Level.INFO);
 * }</pre>
 *
 * <p>An implementation that throws is not propagated into the engine — a flush must not fail
 * because logging it did — but the exception is swallowed rather than reported, so it will be
 * invisible. Do not rely on it.
 */
@FunctionalInterface
public interface ElysiumKVLogger {

    void log(Level level, Event event, String message);

    /** Ordered; a sink receives everything at or above the level passed to the options. */
    enum Level {
        DEBUG,
        INFO,
        WARN,
        ERROR,
        /** Nothing is emitted, and no message is formatted. */
        OFF;

        static Level of(int ordinal) {
            Level[] values = values();
            return ordinal >= 0 && ordinal < values.length ? values[ordinal] : INFO;
        }
    }

    /**
     * What happened, separate from the message, so a sink can route or count without parsing text.
     * A code this binding does not know maps to {@link #UNKNOWN} rather than throwing: the engine
     * may be newer than the jar.
     */
    enum Event {
        FLUSH_COMPLETE,
        COMPACTION_COMPLETE,
        COMPACTION_FAILED,
        MIGRATION_COMPLETE,
        BACKGROUND_FAILURE,
        BACKGROUND_RETRY,
        STALL_ENTERED,
        STALL_LEFT,
        STORES_DISCARDED,
        FENCED,
        GENERATION_ROLLED,
        ORPHANS_RECLAIMED,
        UNKNOWN;

        static Event of(int ordinal) {
            Event[] values = values();
            return ordinal >= 0 && ordinal < values.length - 1 ? values[ordinal] : UNKNOWN;
        }
    }
}
