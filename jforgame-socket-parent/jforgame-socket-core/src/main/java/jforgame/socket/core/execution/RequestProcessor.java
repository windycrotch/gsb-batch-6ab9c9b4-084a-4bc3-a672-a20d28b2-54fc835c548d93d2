package jforgame.socket.core.execution;

import jforgame.commons.reflection.MethodCaller;
import jforgame.commons.reflection.MethodHandleUtils;
import jforgame.socket.core.dispatch.RequestContext;
import jforgame.socket.core.protocol.message.FailureResponse;
import jforgame.socket.core.session.IdSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request processor that executes preprocessed requests on business threads.
 * <p>
 * Tasks submitted by {@link jforgame.socket.core.dispatch.RequestScheduler} invoke this processor on
 * a business thread. It runs before-execution interceptors, invokes the handler,
 * sends any non-null response, and runs after-execution interceptors, with common execution error handling.
 * This flow runs synchronously on the calling thread. The processor does not select threads, create tasks,
 * or enqueue work; callers must invoke it on a business thread to avoid blocking the network IO thread.
 * </p>
 * <p>
 * Processing stops when a before-execution interceptor returns {@code false}. After-execution interceptors
 * run only after successful handler invocation and response sending; they are not guaranteed cleanup callbacks.
 * Instances may be shared across schedulers. Injected interceptors and response senders must support concurrent calls.
 * </p>
 * <p>
 * If the handling method throws and no interceptor claims the exception, the processor answers the client
 * with an identifiable {@link FailureResponse} carrying the original request sequence, instead of only
 * logging. The receipt is delivered through {@link RequestFailureHandler} which swallows any secondary
 * send error, so a failed receipt never disturbs processing of later requests.
 * </p>
 *
 * @since 4.0.0
 */
public final class RequestProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RequestProcessor.class);

    private final RequestResponseSender responseSender;

    private final RequestExecutionInterceptor[] interceptors;

    private final RequestFailureHandler failureHandler;

    public RequestProcessor(RequestResponseSender responseSender, RequestExecutionInterceptor... interceptors) {
        this(responseSender, DefaultRequestFailureHandler.INSTANCE, interceptors);
    }

    public RequestProcessor(RequestResponseSender responseSender, RequestFailureHandler failureHandler,
                            RequestExecutionInterceptor... interceptors) {
        this.responseSender = responseSender;
        this.failureHandler = failureHandler == null ? DefaultRequestFailureHandler.INSTANCE : failureHandler;
        this.interceptors = interceptors == null ? new RequestExecutionInterceptor[0] : interceptors.clone();
    }

    /**
     * Processes a request synchronously on the current business thread. This method does not switch threads
     * and should be invoked by a task submitted by the scheduler.
     *
     * @param session socket session
     * @param requestContext request context with routing and method argument binding already completed
     */
    public void process(IdSession session, RequestContext requestContext) {
        try {
            for (RequestExecutionInterceptor interceptor : interceptors) {
                if (!interceptor.beforeExecute(session, requestContext)) {
                    return;
                }
            }

            MethodCaller methodCaller = MethodHandleUtils.getCaller(requestContext.getMethodExecutor().getMethod());
            Object response = methodCaller.invoke(requestContext.getMethodExecutor().getHandler(), requestContext.getParams());
            if (response != null) {
                requestContext.setResponse(response);
                responseSender.send(session, requestContext);
            }

            for (RequestExecutionInterceptor interceptor : interceptors) {
                interceptor.afterExecute(session, requestContext);
            }
        } catch (Throwable e) {
            requestContext.setError(e);
            if (handleException(session, requestContext, e)) {
                return;
            }
            logger.error("message task execute failed ", e);
            // No interceptor handled it: give the client an identifiable failure receipt
            // carrying the original sequence rather than leaving the request unanswered.
            // Guard the handler itself so a broken receipt can never break the worker thread.
            try {
                failureHandler.handleFailure(session, requestContext, FailureResponse.CODE_BUSINESS_ERROR, e);
            } catch (Throwable sendError) {
                logger.error("failure handler threw while reporting a business error", sendError);
            }
        }
    }

    private boolean handleException(IdSession session, RequestContext requestContext, Throwable throwable) {
        for (RequestExecutionInterceptor interceptor : interceptors) {
            try {
                if (interceptor.handleException(session, requestContext, throwable)) {
                    return true;
                }
            } catch (Throwable ex) {
                logger.error("request interceptor handle exception failed ", ex);
            }
        }
        return false;
    }
}
