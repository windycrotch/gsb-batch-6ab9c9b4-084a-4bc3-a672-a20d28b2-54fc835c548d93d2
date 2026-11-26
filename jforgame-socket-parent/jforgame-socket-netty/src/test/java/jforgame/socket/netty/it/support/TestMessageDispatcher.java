package jforgame.socket.netty.it.support;

import jforgame.socket.core.dispatch.ChainedMessageDispatcher;
import jforgame.socket.core.dispatch.IoMessageFilter;
import jforgame.socket.core.dispatch.PreprocessingIoMessageFilter;
import jforgame.socket.core.dispatch.RequestScheduler;
import jforgame.socket.core.execution.DefaultRequestFailureHandler;
import jforgame.socket.core.protocol.message.MessageFactory;
import jforgame.socket.core.registry.CommonMessageHandlerRegister;
import jforgame.socket.core.registry.MessageHandlerRegister;
import jforgame.socket.core.session.IdSession;
import jforgame.socket.netty.it.msg.ReqFilterError;
import jforgame.socket.threadmodel.RequestSchedulers;
import jforgame.threadmodel.dispatch.DispatchThreadModel;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dispatcher used by the integration tests. It assembles the same building blocks a real server uses:
 * a preprocessing filter (routing + argument binding), a filter that can throw, and a scheduler that
 * hands work to a real {@link DispatchThreadModel} where {@code RequestProcessor} runs the handler.
 */
public class TestMessageDispatcher extends ChainedMessageDispatcher {

    /** Every session ever created, keyed by session id. */
    private final Set<String> createdSessionIds =
            Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Live sessions indexed by creation order, for tests that drive close/dispatch directly. */
    private final java.util.List<IdSession> liveSessions =
            Collections.synchronizedList(new java.util.ArrayList<>());

    /** Number of filter-chain entries actually reached (incremented by the throwing filter only on entry). */
    private final AtomicInteger filterEntered = new AtomicInteger();

    /** One-time cleanup callback invocations; must stay <= 1 per session even under repeated closes. */
    private final AtomicInteger cleanupCount = new AtomicInteger();

    public TestMessageDispatcher(MessageFactory messageFactory, DispatchThreadModel threadModel) {
        MessageHandlerRegister handlerRegister =
                new CommonMessageHandlerRegister(Collections.singletonList(new TestController()), messageFactory);

        // Real preprocessing filter: resolves the handler and binds method arguments.
        addIoFilter(new PreprocessingIoMessageFilter(messageFactory, handlerRegister));

        // A filter that simulates a filter-chain failure for the dedicated request type.
        IoMessageFilter failingFilter = (session, context) -> {
            filterEntered.incrementAndGet();
            if (context.getRequest() instanceof ReqFilterError) {
                throw new IllegalStateException("boom from io filter");
            }
            return true;
        };
        addIoFilter(failingFilter);

        // Real scheduler -> business thread model -> RequestProcessor (with default failure receipt handler).
        RequestScheduler scheduler = RequestSchedulers.newDispatchScheduler(
                threadModel,
                context -> context.getSession().hashCode(),
                new jforgame.socket.core.support.SessionResponseSender(),
                DefaultRequestFailureHandler.INSTANCE);
        addIoFilter((session, context) -> {
            scheduler.schedule(session, context);
            return true;
        });
    }

    @Override
    public void onSessionCreated(IdSession session) {
        createdSessionIds.add(session.getId());
        liveSessions.add(session);
    }

    @Override
    protected void doSessionClosed(IdSession session) {
        cleanupCount.incrementAndGet();
    }

    /**
     * @return the most recently created live session
     */
    public IdSession lastSession() {
        synchronized (liveSessions) {
            return liveSessions.isEmpty() ? null : liveSessions.get(liveSessions.size() - 1);
        }
    }

    public int getCleanupCount() {
        return cleanupCount.get();
    }

    public int getFilterEntered() {
        return filterEntered.get();
    }

    public Set<String> getCreatedSessionIds() {
        return createdSessionIds;
    }
}
