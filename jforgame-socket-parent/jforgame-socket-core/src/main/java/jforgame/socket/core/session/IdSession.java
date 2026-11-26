package jforgame.socket.core.session;

import jforgame.socket.core.protocol.message.SocketDataFrame;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Session interface, used to represent a socket connection session.
 * This interface defines basic session operations, such as sending messages, closing sessions, getting session attributes, etc.
 * A session refers to a communication between client and server, containing connection information and session attributes.
 * The lifecycle of a session starts when the client connects to the server and ends when the client disconnects or the server actively closes the session.
 * Sessions can also contain some extended attributes for storing session-related information.
 * Classes implementing this interface can be session classes from network frameworks like Netty or Mina.
 */
public interface IdSession extends Closeable {

    @Deprecated
    String ID = "ID";

    /**
     * get a unique session identifier (read only)
     *
     * @return Return a unique session identifier
     */
    String getId();

    /**
     * get session owner id
     *
     * @return owner id
     */
    String getOwnerId();

    /**
     * set session owner id
     *
     * @param ownerId owner id
     */
    void setOwnerId(String ownerId);


    /**
     * session message without index
     * the message will be wrapped to {@link SocketDataFrame}
     *
     * @param packet message to send
     */
    void send(Object packet);

    /**
     * session message before close session
     * the message will be wrapped to {@link SocketDataFrame}
     *
     * @param packet message to send
     * @throws IOException if exception
     * @since 2.2.2
     */
    void sendAndClose(Object packet) throws IOException;

    /**
     * send message with index.
     * when client wants to send a message and then gets its response, so-called callback
     *
     * @param index  index of the request message
     * @param packet message to send
     */
    default void send(int index, Object packet) {
        send(SocketDataFrame.withIndex(index, packet));
    }

    InetSocketAddress getRemoteAddress();

    String getRemoteIP();

    int getRemotePort();

    InetSocketAddress getLocalAddress();

    String getLocalIP();

    int getLocalPort();

    /**
     * update session attribute
     *
     * @param key   key of the attribute
     * @param value value of the attribute
     */
    void setAttribute(String key, Object value);

    /**
     * get session attribute
     *
     * @param key target attribute key
     * @return the value associated with the key
     */
    Object getAttribute(String key);

    /**
     * return whether session has value of target key
     *
     * @param key attribute key
     * @return value binding to the key
     */
    default boolean hasAttribute(String key) {
        return getAttribute(key) != null;
    }

    /**
     * nio raw session
     * //     * @see io.netty.channel.Channel
     * //     * @see org.apache.mina.core.session.IoSession
     *
     * @return NSession or MSession
     */
    Object getRawSession();

    /**
     * Whether this session has started closing or is already closed.
     * <p>
     * Returns {@code true} as soon as {@link #close()} has been invoked once (even while the
     * underlying channel is still flushing), or the network connection has been torn down.
     * Late requests observed in this state must be rejected by the dispatcher.
     * </p>
     *
     * @return {@code true} if the session is closing/closed and must no longer accept requests
     */
    default boolean isClosed() {
        return false;
    }

    /**
     * Atomically mark the session as closing.
     * <p>
     * Only the first invocation returns {@code true}; every later invocation returns {@code false},
     * which is the building block of idempotent session shutdown. It does not itself release the
     * underlying connection; callers pair it with {@link #close()}.
     * </p>
     *
     * @return {@code true} if this invocation won the race and performed the first mark;
     *         {@code false} if the session was already marked closing
     */
    default boolean markClosing() {
        return true;
    }

    /**
     * Whether the one-time session cleanup callback has already been executed.
     *
     * @return {@code true} after the cleanup callback has run once
     */
    default boolean isCleanupDone() {
        return false;
    }

    /**
     * Atomically mark that the one-time session cleanup callback has run.
     *
     * @return {@code true} if this invocation performed the first mark and cleanup may proceed;
     *         {@code false} if cleanup had already been marked by another thread
     */
    default boolean markCleanupDone() {
        return true;
    }

}
