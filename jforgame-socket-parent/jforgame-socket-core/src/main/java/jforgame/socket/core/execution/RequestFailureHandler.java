package jforgame.socket.core.execution;

import jforgame.socket.core.dispatch.RequestContext;
import jforgame.socket.core.session.IdSession;

/**
 * Handles the observable failure path of a request.
 * <p>
 * When the IO filter chain or the business handling method throws, the framework asks this handler
 * to deliver a recognizable failure receipt (carrying the original request sequence) back to the client,
 * instead of only writing a server side log.
 * </p>
 * <p>
 * Implementations must be thread safe and must never throw: a failure receipt that itself cannot be
 * delivered must be swallowed and logged, otherwise one broken connection would poison the processing
 * of subsequent requests on the same business thread.
 * </p>
 *
 * @since 5.0.0
 */
public interface RequestFailureHandler {

    /**
     * A no-op handler that keeps the legacy "log only, no receipt" behavior.
     */
    RequestFailureHandler NOOP = (session, requestContext, errorCode, cause) -> {
    };

    /**
     * Deliver the failure receipt for the given request.
     *
     * @param session        socket session the request arrived on
     * @param requestContext request context; the header provides the original index/cmd
     * @param errorCode      framework error code describing where the failure happened
     * @param cause          the throwable that aborted the request, may be {@code null}
     */
    void handleFailure(IdSession session, RequestContext requestContext, int errorCode, Throwable cause);
}
