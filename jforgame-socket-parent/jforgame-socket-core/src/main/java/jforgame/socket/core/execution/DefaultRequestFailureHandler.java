package jforgame.socket.core.execution;

import jforgame.socket.core.dispatch.RequestContext;
import jforgame.socket.core.protocol.message.FailureResponse;
import jforgame.socket.core.protocol.message.MessageHeader;
import jforgame.socket.core.session.IdSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default failure handler that answers the client with a {@link FailureResponse}.
 * <p>
 * Every delivery attempt is fully guarded: if the session is already closed, or encoding/writing
 * the receipt fails, the error is only logged. Exceptions never escape this class, so a failed
 * failure receipt cannot interrupt the worker thread that is processing subsequent requests.
 * </p>
 * <p>
 * The receipt reuses the original request header's sequence number and command, guaranteeing the
 * client can match every failure to the request it sent and never observes crossed sequences.
 * </p>
 *
 * @since 5.0.0
 */
public class DefaultRequestFailureHandler implements RequestFailureHandler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRequestFailureHandler.class);

    public static final DefaultRequestFailureHandler INSTANCE = new DefaultRequestFailureHandler();

    @Override
    public void handleFailure(IdSession session, RequestContext requestContext, int errorCode, Throwable cause) {
        try {
            if (session == null || session.isClosed()) {
                logger.warn("skip failure receipt, session already closed, errorCode={}", errorCode, cause);
                return;
            }
            MessageHeader header = requestContext.getHeader();
            int index = header == null ? 0 : header.getIndex();
            int cmd = header == null ? 0 : header.getCmd();
            String reason = cause == null ? ("errorCode=" + errorCode)
                    : (cause.getClass().getSimpleName() + ": " + cause.getMessage());
            FailureResponse failure = new FailureResponse(index, cmd, errorCode, reason);
            // Send with the original index so the response is correlated one-to-one with the request.
            session.send(index, failure);
            if (logger.isDebugEnabled()) {
                logger.debug("sent failure receipt {} for request index={}, cmd={}", failure, index, cmd);
            }
        } catch (Throwable t) {
            // Sending the failure receipt must never affect processing of later requests.
            logger.error("send failure receipt failed, errorCode={}", errorCode, t);
        }
    }
}
