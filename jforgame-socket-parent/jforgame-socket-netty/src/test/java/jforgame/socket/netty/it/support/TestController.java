package jforgame.socket.netty.it.support;

import jforgame.socket.core.protocol.annotation.MessageRoute;
import jforgame.socket.core.protocol.annotation.RequestHandler;
import jforgame.socket.core.session.IdSession;
import jforgame.socket.netty.it.msg.ReqBusinessError;
import jforgame.socket.netty.it.msg.ReqEcho;
import jforgame.socket.netty.it.msg.ReqFilterError;
import jforgame.socket.netty.it.msg.ReqUnencodable;
import jforgame.socket.netty.it.msg.ResEcho;
import jforgame.socket.netty.it.msg.ResUnencodable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Business handlers backing the integration tests.
 */
@MessageRoute
public class TestController {

    /**
     * Counts every business method invocation, used to prove rejected/closed-session
     * requests never reach business logic.
     */
    public static final AtomicInteger BUSINESS_INVOCATIONS = new AtomicInteger();

    @RequestHandler
    public ResEcho echo(IdSession session, int index, ReqEcho request) {
        BUSINESS_INVOCATIONS.incrementAndGet();
        return new ResEcho(index, request.getContent());
    }

    @RequestHandler
    public Object businessError(IdSession session, int index, ReqBusinessError request) {
        BUSINESS_INVOCATIONS.incrementAndGet();
        throw new IllegalStateException("boom from business handler, index=" + index);
    }

    @RequestHandler
    public ResUnencodable unencodable(IdSession session, int index, ReqUnencodable request) {
        BUSINESS_INVOCATIONS.incrementAndGet();
        // A bare Object is not JSON serializable; the codec throws while encoding the response.
        return new ResUnencodable(index, new Object());
    }

    /**
     * Registered only so routing/preprocessing succeeds for {@link ReqFilterError}; the throwing
     * IO filter aborts the chain before this method can ever run.
     */
    @RequestHandler
    public Object filterErrorTarget(IdSession session, int index, ReqFilterError request) {
        BUSINESS_INVOCATIONS.incrementAndGet();
        throw new AssertionError("must never reach business logic when the filter chain throws");
    }
}
