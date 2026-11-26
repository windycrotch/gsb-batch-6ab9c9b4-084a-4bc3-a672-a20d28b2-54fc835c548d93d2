package jforgame.socket.core.session;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Skeleton {@link IdSession} implementing thread safe, idempotent lifecycle state.
 * <p>
 * Two independent states are tracked with CAS flags:
 * </p>
 * <ul>
 *     <li><b>accepting state</b> ({@link #markClosing()} / {@link #isClosed()}): once a close has been
 *     initiated (either by the server calling {@link #close()} or by the network connection dropping),
 *     no further request may enter the filter chain or business logic.</li>
 *     <li><b>cleanup state</b> ({@link #markCleanupDone()} / {@link #isCleanupDone()}): guarantees the
 *     expensive session cleanup callback runs exactly once even when close events and real disconnect
 *     events interleave.</li>
 * </ul>
 * <p>
 * Concrete sessions (Netty/Mina) only need to implement the actual transport operations and call
 * {@link #markClosing()} from their {@link #close()} implementation.
 * </p>
 *
 * @since 5.0.0
 */
public abstract class AbstractIdSession implements IdSession {

    /**
     * False while the session is accepting requests; flipped to true exactly once when closing starts.
     */
    private final AtomicBoolean closing = new AtomicBoolean(false);

    /**
     * False until the one-time session cleanup callback has been claimed.
     */
    private final AtomicBoolean cleanupDone = new AtomicBoolean(false);

    @Override
    public boolean isClosed() {
        return closing.get();
    }

    @Override
    public boolean markClosing() {
        return closing.compareAndSet(false, true);
    }

    @Override
    public boolean isCleanupDone() {
        return cleanupDone.get();
    }

    @Override
    public boolean markCleanupDone() {
        return cleanupDone.compareAndSet(false, true);
    }
}
