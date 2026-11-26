package jforgame.socket.core.dispatch;

import jforgame.socket.core.execution.DefaultRequestFailureHandler;
import jforgame.socket.core.execution.RequestFailureHandler;
import jforgame.socket.core.protocol.message.FailureResponse;
import jforgame.socket.core.session.IdSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides a skeletal implementation of {@link SocketIoDispatcher}.
 * It enhances the {@link SocketIoDispatcher#dispatch(IdSession, RequestContext)} method in a chain pattern.
 * Each ChainedMessageDispatcher may have multiple message handler nodes.
 * When a message is passed to a message handler, the handler can choose to pass the message to the next node or stop the message propagation.
 *
 * @see IoMessageFilter#filter(IdSession, RequestContext)
 */
public abstract class ChainedMessageDispatcher implements SocketIoDispatcher {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Chain of message handlers
     * Iterates through handlers in the chain, stops and returns if any handler returns false
     */
    protected List<IoMessageFilter> dispatchChain = new ArrayList<>();

    /**
     * Delivers an observable failure receipt when a filter or the business handler throws.
     * Defaults to {@link DefaultRequestFailureHandler}; may be replaced (including
     * {@link RequestFailureHandler#NOOP}) before the server starts.
     */
    private RequestFailureHandler failureHandler = DefaultRequestFailureHandler.INSTANCE;

    /**
     * Adds a message filter to the end of the current chain
     * @param filter message handler
     */
    public void addIoFilter(IoMessageFilter filter) {
        this.dispatchChain.add(filter);
    }

    /**
     * Configure how failed requests are reported back to the client.
     *
     * @param failureHandler failure handler, never {@code null}
     */
    public void setFailureHandler(RequestFailureHandler failureHandler) {
        if (failureHandler == null) {
            throw new IllegalArgumentException("failureHandler must not be null");
        }
        this.failureHandler = failureHandler;
    }

    @Override
    public void dispatch(IdSession session, RequestContext context) {
        // Requests arriving once the session is closing/closed are rejected outright:
        // they never enter the filter chain or business logic and produce no response.
        if (session == null || session.isClosed()) {
            if (logger.isDebugEnabled()) {
                logger.debug("reject request on closed session, header={}",
                        context == null ? null : context.getHeader());
            }
            return;
        }
        for (IoMessageFilter ioMessageFilter : dispatchChain) {
            try {
                if (!ioMessageFilter.filter(session, context)) {
                    break;
                }
            } catch (Exception e) {
                logger.error("io message filter [{}] failed, reply failure receipt",
                        ioMessageFilter.getClass().getName(), e);
                // Make the failure observable to the client instead of silently dropping the request.
                // Guard the handler itself: a failure receipt must never escape into the IO pipeline.
                try {
                    failureHandler.handleFailure(session, context, FailureResponse.CODE_FILTER_ERROR, e);
                } catch (Throwable sendError) {
                    logger.error("failure handler threw while reporting a filter error", sendError);
                }
                break;
            }
        }
    }

    /**
     * Session close entry, idempotent under concurrent close/disconnect events.
     * <p>
     * The actual cleanup is delegated to {@link #doSessionClosed(IdSession)}, which is guaranteed
     * to run at most once per session even if this method is invoked repeatedly (idle timeout close,
     * explicit business close and the underlying channel inactive event can all race here).
     * </p>
     * <p>
     * Subclasses extend cleanup behavior by overriding {@link #doSessionClosed(IdSession)}, not this method.
     * </p>
     */
    @Override
    public final void onSessionClosed(IdSession session) {
        if (session == null) {
            return;
        }
        // Stop accepting late requests immediately, regardless of who triggered the close.
        session.markClosing();
        if (!session.markCleanupDone()) {
            if (logger.isDebugEnabled()) {
                logger.debug("session [{}] cleanup already done, skip duplicate close", session.getId());
            }
            return;
        }
        try {
            doSessionClosed(session);
        } catch (Throwable t) {
            logger.error("session [{}] cleanup failed", session.getId(), t);
        }
    }

    /**
     * One-time session cleanup extension point, invoked by {@link #onSessionClosed(IdSession)} exactly once.
     * <p>
     * Heavy resource release (offline persistence, attribute eviction, ...) should be implemented here.
     * The default implementation does nothing.
     * </p>
     *
     * @param session the session being closed
     */
    protected void doSessionClosed(IdSession session) {
    }

    @Override
    public void exceptionCaught(IdSession session, Throwable cause) {
        logger.error("", cause);
    }

}
