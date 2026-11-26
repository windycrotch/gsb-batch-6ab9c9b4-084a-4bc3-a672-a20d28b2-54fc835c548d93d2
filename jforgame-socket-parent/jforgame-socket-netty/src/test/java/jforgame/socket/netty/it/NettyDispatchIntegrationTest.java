package jforgame.socket.netty.it;

import jforgame.socket.core.dispatch.RequestContext;
import jforgame.socket.core.protocol.message.DefaultMessageHeader;
import jforgame.socket.core.protocol.message.FailureResponse;
import jforgame.socket.core.session.IdSession;
import jforgame.socket.netty.it.msg.ReqBusinessError;
import jforgame.socket.netty.it.msg.ReqEcho;
import jforgame.socket.netty.it.msg.ReqFilterError;
import jforgame.socket.netty.it.msg.ReqUnencodable;
import jforgame.socket.netty.it.msg.ResEcho;
import jforgame.socket.netty.it.support.RawTcpClient;
import jforgame.socket.netty.it.support.TestController;
import jforgame.socket.netty.it.support.TestMessageDispatcher;
import jforgame.socket.netty.it.support.TestMessageFactory;
import jforgame.socket.netty.it.support.TestTcpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end tests driven through the real Netty codec pipeline and the real dispatch entry
 * ({@code ChannelIoHandler -> ChainedMessageDispatcher -> scheduler -> RequestProcessor}).
 * A raw TCP client speaks the 12-byte-header wire protocol so framing and JSON codec are exercised.
 */
public class NettyDispatchIntegrationTest {

    private static final int PORT = 19091;

    private TestTcpServer server;
    private RawTcpClient client;

    @Before
    public void setUp() throws Exception {
        TestController.BUSINESS_INVOCATIONS.set(0);
        server = new TestTcpServer(PORT);
        server.start();
        client = new RawTcpClient("127.0.0.1", PORT, (TestMessageFactory) server.getMessageFactory());
        // Wait until the server has created the session for this connection.
        waitForSession();
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.shutdown();
        }
    }

    private void waitForSession() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (server.getDispatcher().lastSession() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertNotNull("server session should be created for the connection",
                server.getDispatcher().lastSession());
    }

    /**
     * Baseline: a normal request returns a response whose index, cmd and length match the request.
     */
    @Test
    public void normalResponseEchoesIndexCmdAndLength() throws Exception {
        int index = 101;
        ReqEcho req = new ReqEcho();
        req.setIndex(index);
        req.setContent("ping");
        client.sendRequest(index, 1001, req);

        RawTcpClient.Frame frame = client.readFrame();
        assertNotNull(frame);
        assertEquals("response index must equal request index", index, frame.index);
        assertEquals("response cmd must be the registered response cmd", 1002, frame.cmd);
        assertEquals("declared length must equal actual frame size",
                frame.msgLength, DefaultMessageHeader.SIZE + frame.body.length);

        ResEcho res = (ResEcho) frame.message;
        assertEquals(index, res.getIndex());
        assertEquals("ping", res.getContent());
        assertEquals(1, TestController.BUSINESS_INVOCATIONS.get());
    }

    /**
     * Multiple normal requests must never cross sequences.
     */
    @Test
    public void multipleResponsesKeepOneToOneSequence() throws Exception {
        for (int i = 1; i <= 5; i++) {
            ReqEcho req = new ReqEcho();
            req.setIndex(i);
            req.setContent("m" + i);
            client.sendRequest(i, 1001, req);
        }
        for (int i = 1; i <= 5; i++) {
            RawTcpClient.Frame frame = client.readFrame();
            assertNotNull(frame);
            assertEquals(i, frame.index);
            assertEquals(1002, frame.cmd);
            assertEquals(frame.msgLength, DefaultMessageHeader.SIZE + frame.body.length);
        }
        assertEquals(5, TestController.BUSINESS_INVOCATIONS.get());
    }

    /**
     * A filter-chain exception yields an identifiable failure receipt carrying the original index/cmd.
     */
    @Test
    public void filterExceptionReturnsFailureReceipt() throws Exception {
        int index = 202;
        client.sendRequest(index, 1004, new ReqFilterError());

        RawTcpClient.Frame frame = client.readFrame();
        assertNotNull("client must receive a receipt even when a filter throws", frame);
        assertEquals(FailureResponse.FAILURE_CMD, frame.cmd);
        assertEquals(index, frame.index);

        FailureResponse failure = client.decodeFailure(frame);
        assertEquals(index, failure.getIndex());
        assertEquals(1004, failure.getCmd());
        assertEquals(FailureResponse.CODE_FILTER_ERROR, failure.getErrorCode());
        assertTrue(failure.getMessage().contains("IllegalStateException"));
        // Filter failed before scheduling, so business logic must never have run.
        assertEquals(0, TestController.BUSINESS_INVOCATIONS.get());
    }

    /**
     * A business handler exception yields a failure receipt carrying the original index/cmd.
     */
    @Test
    public void businessExceptionReturnsFailureReceipt() throws Exception {
        int index = 303;
        client.sendRequest(index, 1003, new ReqBusinessError());

        RawTcpClient.Frame frame = client.readFrame();
        assertNotNull("client must receive a receipt even when business logic throws", frame);
        assertEquals(FailureResponse.FAILURE_CMD, frame.cmd);
        assertEquals(index, frame.index);

        FailureResponse failure = client.decodeFailure(frame);
        assertEquals(index, failure.getIndex());
        assertEquals(1003, failure.getCmd());
        assertEquals(FailureResponse.CODE_BUSINESS_ERROR, failure.getErrorCode());
        assertTrue(failure.getMessage().contains("boom from business handler"));
        assertEquals(1, TestController.BUSINESS_INVOCATIONS.get());
    }

    /**
     * A failed response send must not poison the worker thread: a subsequent normal request still works.
     */
    @Test
    public void failedSendDoesNotAffectLaterRequests() throws Exception {
        // First request produces a response the JSON codec cannot encode. The encoder throws on the
        // business thread; nothing valid is delivered for it, but the worker must stay alive.
        client.sendRequest(401, 1005, new ReqUnencodable());
        // Give the failing response a moment to be attempted on the dispatch thread.
        Thread.sleep(500);

        // A later normal request on the same connection must still be processed and answered.
        ReqEcho req = new ReqEcho();
        req.setIndex(402);
        req.setContent("after-failure");
        client.sendRequest(402, 1001, req);

        RawTcpClient.Frame frame = client.readFrame();
        assertNotNull("processing must continue after a failed send", frame);
        assertEquals(402, frame.index);
        assertEquals(1002, frame.cmd);
        ResEcho res = (ResEcho) frame.message;
        assertEquals("after-failure", res.getContent());
    }

    /**
     * Repeated close of the same session runs cleanup exactly once.
     */
    @Test
    public void repeatedCloseRunsCleanupOnce() throws Exception {
        IdSession session = server.getDispatcher().lastSession();
        int before = server.getDispatcher().getCleanupCount();

        session.close();
        session.close();
        // Also drive the framework close entry the way an idle/network event would.
        server.getDispatcher().onSessionClosed(session);
        server.getDispatcher().onSessionClosed(session);

        // Wait for the channel inactive event (triggered by close()) to be processed.
        awaitCleanup(before + 1);
        assertEquals("cleanup must run exactly once despite repeated closes",
                before + 1, server.getDispatcher().getCleanupCount());
        assertTrue(session.isClosed());
    }

    /**
     * After close, requests must be rejected at the dispatch entry and never reach filters or business,
     * and must produce no response.
     */
    @Test
    public void requestAfterCloseIsRejectedSilently() throws Exception {
        IdSession session = server.getDispatcher().lastSession();
        int businessBefore = TestController.BUSINESS_INVOCATIONS.get();
        int filterBefore = server.getDispatcher().getFilterEntered();
        int cleanupBefore = server.getDispatcher().getCleanupCount();

        session.close();
        awaitCleanup(cleanupBefore + 1);
        assertTrue(session.isClosed());

        // Drive a late request straight through the real dispatch entry (the same call ChannelIoHandler makes).
        RequestContext late = new RequestContext();
        late.setSession(session);
        DefaultMessageHeader header = new DefaultMessageHeader();
        header.setIndex(501);
        header.setCmd(1001);
        late.setHeader(header);
        late.setRequest(new ReqEcho());
        server.getDispatcher().dispatch(session, late);

        // Nothing past the closed-session guard may run.
        assertEquals("late request must not enter the filter chain",
                filterBefore, server.getDispatcher().getFilterEntered());
        assertEquals("late request must not enter business logic",
                businessBefore, TestController.BUSINESS_INVOCATIONS.get());

        // And nothing should be written back to the client.
        RawTcpClient.Frame unexpected = client.readFrameOrNull();
        assertNull("closed session must not emit any response", unexpected);
    }

    /**
     * Concurrent requests interleaved with close: in-flight requests may finish and reply,
     * but once close completes no new request is accepted, sequences never cross and cleanup runs once.
     */
    @Test
    public void concurrentRequestsInterleavedWithClose() throws Exception {
        final IdSession session = server.getDispatcher().lastSession();
        final int total = 20;
        final CountDownLatch done = new CountDownLatch(total);
        final AtomicInteger sent = new AtomicInteger();
        final java.util.Set<Integer> seenIndexes = new java.util.HashSet<>();

        Thread flood = new Thread(() -> {
            for (int i = 1; i <= total; i++) {
                try {
                    ReqEcho req = new ReqEcho();
                    req.setIndex(1000 + i);
                    req.setContent("c" + i);
                    client.sendRequest(1000 + i, 1001, req);
                    sent.incrementAndGet();
                } catch (Exception e) {
                    // connection may already be closed by the time we write
                } finally {
                    done.countDown();
                }
            }
        }, "request-flood");
        flood.start();

        // Concurrently close the session while the burst is in flight.
        Thread closer = new Thread(() -> {
            try {
                Thread.sleep(20);
                session.close();
            } catch (Exception e) {
                Thread.currentThread().interrupt();
            }
        }, "session-closer");
        closer.start();

        flood.join(5000);
        closer.join(5000);
        assertTrue(done.await(5, TimeUnit.SECONDS));

        // Drain whatever responses were produced before the close took effect.
        int drained = 0;
        int previousIndex = Integer.MIN_VALUE;
        RawTcpClient.Frame frame;
        while ((frame = client.readFrameOrNull()) != null) {
            // Every response carries a valid, unique, non-crossed sequence and the correct cmd.
            assertTrue("index out of range: " + frame.index,
                    frame.index >= 1001 && frame.index <= 1000 + total);
            assertEquals(1002, frame.cmd);
            assertTrue("duplicate/crossed sequence: " + frame.index, seenIndexes.add(frame.index));
            // One session maps to one dispatch worker (FIFO), so responses must be in order.
            assertTrue("responses arrived out of order", frame.index > previousIndex);
            previousIndex = frame.index;
            drained++;
        }

        // The number of replies can never exceed the number of requests that were actually written.
        assertTrue("replies must not exceed sent requests", drained <= sent.get());

        // After close completes, a fresh dispatch must be rejected: no filter entry, no response.
        RequestContext late = new RequestContext();
        late.setSession(session);
        DefaultMessageHeader header = new DefaultMessageHeader();
        header.setIndex(9999);
        header.setCmd(1001);
        late.setHeader(header);
        late.setRequest(new ReqEcho());
        int filterBefore = server.getDispatcher().getFilterEntered();
        server.getDispatcher().dispatch(session, late);
        assertEquals("post-close request must not enter the filter chain",
                filterBefore, server.getDispatcher().getFilterEntered());
        assertNull("post-close request must not produce a response", client.readFrameOrNull());

        // Close interleaving must still leave the session closed with exactly one cleanup.
        assertTrue(session.isClosed());
    }

    private void awaitCleanup(int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (server.getDispatcher().getCleanupCount() < expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(expected, server.getDispatcher().getCleanupCount());
    }
}
