package jforgame.socket.netty.it;

import jforgame.socket.core.dispatch.RequestContext;
import jforgame.socket.core.execution.DefaultRequestFailureHandler;
import jforgame.socket.core.execution.RequestProcessor;
import jforgame.socket.core.execution.RequestResponseSender;
import jforgame.socket.core.protocol.message.DefaultMessageHeader;
import jforgame.socket.core.protocol.message.FailureResponse;
import jforgame.socket.core.protocol.message.SocketDataFrame;
import jforgame.socket.core.registry.MessageExecutor;
import jforgame.socket.core.registry.MessageHandlerMethod;
import jforgame.socket.core.session.AbstractIdSession;
import jforgame.socket.netty.it.msg.ResEcho;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Focused tests for the defensive guarantees that are awkward to assert through the wire:
 * <ul>
 *     <li>a failure receipt whose own send throws must never propagate (it cannot poison later requests);</li>
 *     <li>no receipt is attempted on an already-closed session;</li>
 *     <li>the abstract session's closing/cleanup CAS flags are idempotent under repeated calls.</li>
 * </ul>
 */
public class FailureHandlerAndSessionLifecycleTest {

    /** Minimal in-memory IdSession whose send can be instructed to throw. */
    private static class StubSession extends AbstractIdSession {
        final AtomicInteger sendCalls = new AtomicInteger();
        volatile boolean throwOnSend;
        volatile SocketDataFrame lastFrame;

        @Override
        public String getId() {
            return "stub";
        }

        @Override
        public String getOwnerId() {
            return null;
        }

        @Override
        public void setOwnerId(String ownerId) {
        }

        @Override
        public void send(Object packet) {
            sendCalls.incrementAndGet();
            lastFrame = (SocketDataFrame) packet;
            if (throwOnSend) {
                throw new RuntimeException("simulated broken channel write");
            }
        }

        @Override
        public void sendAndClose(Object packet) {
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return null;
        }

        @Override
        public String getRemoteIP() {
            return null;
        }

        @Override
        public int getRemotePort() {
            return 0;
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return null;
        }

        @Override
        public String getLocalIP() {
            return null;
        }

        @Override
        public int getLocalPort() {
            return 0;
        }

        @Override
        public void setAttribute(String key, Object value) {
        }

        @Override
        public Object getAttribute(String key) {
            return null;
        }

        @Override
        public Object getRawSession() {
            return null;
        }

        @Override
        public void close() {
            markClosing();
        }
    }

    private RequestContext context(int index, int cmd) {
        RequestContext ctx = new RequestContext();
        DefaultMessageHeader header = new DefaultMessageHeader();
        header.setIndex(index);
        header.setCmd(cmd);
        ctx.setHeader(header);
        ctx.setRequest(new ResEcho(index, "x"));
        return ctx;
    }

    /** The handler must swallow a broken-send error instead of letting it reach the worker thread. */
    @Test
    public void failedReceiptSendDoesNotThrow() {
        StubSession session = new StubSession();
        session.throwOnSend = true;

        DefaultRequestFailureHandler.INSTANCE.handleFailure(
                session, context(77, 1001), FailureResponse.CODE_BUSINESS_ERROR,
                new IllegalStateException("boom"));

        // The attempt happened, but the exception was absorbed: the test reaches here without failing.
        assertEquals(1, session.sendCalls.get());
    }

    /** After the first broken receipt, subsequent handling on the same session still works. */
    @Test
    public void laterReceiptStillDeliveredAfterOneFailure() {
        StubSession session = new StubSession();
        session.throwOnSend = true;
        DefaultRequestFailureHandler.INSTANCE.handleFailure(
                session, context(1, 1001), FailureResponse.CODE_BUSINESS_ERROR, new RuntimeException());

        session.throwOnSend = false;
        DefaultRequestFailureHandler.INSTANCE.handleFailure(
                session, context(2, 1001), FailureResponse.CODE_FILTER_ERROR, new RuntimeException());

        assertEquals(2, session.sendCalls.get());
        SocketDataFrame frame = session.lastFrame;
        assertEquals(2, frame.getIndex());
        assertTrue(frame.getMessage() instanceof FailureResponse);
        assertEquals(2, ((FailureResponse) frame.getMessage()).getIndex());
    }

    /** No receipt may be written once the session is closed. */
    @Test
    public void closedSessionGetsNoReceipt() {
        StubSession session = new StubSession();
        session.close();
        assertTrue(session.isClosed());

        DefaultRequestFailureHandler.INSTANCE.handleFailure(
                session, context(9, 1001), FailureResponse.CODE_BUSINESS_ERROR, new RuntimeException());

        assertEquals(0, session.sendCalls.get());
    }

    /** Lifecycle CAS flags must give exactly one winning transition no matter how often they are hit. */
    @Test
    public void lifecycleFlagsAreIdempotent() {
        StubSession session = new StubSession();
        assertFalse(session.isClosed());
        assertTrue(session.markClosing());
        // Every further mark loses the race.
        assertFalse(session.markClosing());
        assertFalse(session.markClosing());
        assertTrue(session.isClosed());

        assertFalse(session.isCleanupDone());
        assertTrue(session.markCleanupDone());
        assertFalse(session.markCleanupDone());
        assertTrue(session.isCleanupDone());
    }

    /** The receipt always carries the original index/cmd and a recognizable failure command payload. */
    @Test
    public void receiptCarriesOriginalSequenceAndCmd() {
        StubSession session = new StubSession();
        DefaultRequestFailureHandler.INSTANCE.handleFailure(
                session, context(321, 1001), FailureResponse.CODE_FILTER_ERROR,
                new IOException("filter blew up"));

        SocketDataFrame frame = session.lastFrame;
        assertEquals(321, frame.getIndex());
        FailureResponse failure = (FailureResponse) frame.getMessage();
        assertEquals(321, failure.getIndex());
        assertEquals(1001, failure.getCmd());
        assertEquals(FailureResponse.CODE_FILTER_ERROR, failure.getErrorCode());
        assertTrue(failure.getMessage().contains("filter blew up"));
    }

    /** Handler used by the RequestProcessor test; its method always throws. */
    public static class ThrowingBusinessHandler {
        public Object explode(StubSession session, int index, Object request) {
            throw new IllegalStateException("business blew up, index=" + index);
        }
    }

    /**
     * Drives the real {@link RequestProcessor}: when the business method throws and the configured
     * failure handler also throws, {@code process()} must swallow the secondary error and return
     * normally, so the calling worker thread stays alive for subsequent requests.
     */
    @Test
    public void processorAbsorbsFailureHandlerThrowAndStaysUsable() throws Exception {
        StubSession session = new StubSession();
        Method method = ThrowingBusinessHandler.class.getMethod("explode", StubSession.class, int.class, Object.class);
        MessageExecutor executor = MessageHandlerMethod.valueOf(
                method, method.getParameterTypes(), new ThrowingBusinessHandler());

        AtomicInteger brokenCalls = new AtomicInteger();
        // A failure handler that itself fails, like a write on a broken channel.
        jforgame.socket.core.execution.RequestFailureHandler brokenFailureHandler =
                (s, ctx, errorCode, cause) -> {
                    brokenCalls.incrementAndGet();
                    throw new RuntimeException("failure receipt write failed");
                };
        RequestResponseSender sender = (s, ctx) -> {
            throw new AssertionError("normal sender must not be used when the business method threw");
        };
        RequestProcessor processor = new RequestProcessor(sender, brokenFailureHandler);

        // First call: business throws, then the broken failure handler throws. Nothing must escape.
        processor.process(session, businessContext(executor, 701));
        assertEquals(1, brokenCalls.get());

        // The processor (and its worker thread) must still be usable for the next request.
        processor.process(session, businessContext(executor, 702));
        assertEquals(2, brokenCalls.get());
    }

    private RequestContext businessContext(MessageExecutor executor, int index) {
        RequestContext ctx = new RequestContext();
        DefaultMessageHeader header = new DefaultMessageHeader();
        header.setIndex(index);
        header.setCmd(1003);
        ctx.setHeader(header);
        ctx.setRequest(new Object());
        ctx.setMethodExecutor(executor);
        ctx.setParams(new Object[]{new StubSession(), index, new Object()});
        return ctx;
    }
}
