package jforgame.socket.core.protocol.message;

/**
 * Framework level failure response.
 * <p>
 * When a request fails inside the filter chain or inside the business handling method,
 * the framework answers the client with this recognizable packet instead of silently dropping the request.
 * The packet always carries the original client request sequence ({@code index}) so the client can
 * correlate the failure with the exact request it sent, together with the original command number,
 * a machine readable error code and a human readable error message.
 * </p>
 * <p>
 * The command number is a framework reserved value ({@link #FAILURE_CMD}) and must not be occupied
 * by any business message. Applications register this class in their {@link MessageFactory}
 * (see {@link #registerTo(MessageFactory)}) so that the encoder can serialize it on the wire.
 * </p>
 *
 * @since 5.0.0
 */
public class FailureResponse implements Message {

    /**
     * Framework reserved command number for failure responses. Business messages must not use it.
     */
    public static final int FAILURE_CMD = 60000;

    /**
     * Error code indicating the failure happened in the IO filter chain.
     */
    public static final int CODE_FILTER_ERROR = 1;

    /**
     * Error code indicating the failure happened in the business handling method.
     */
    public static final int CODE_BUSINESS_ERROR = 2;

    /**
     * Original client request sequence number, echoed back verbatim.
     */
    private int index;

    /**
     * Original command number of the failed request.
     */
    private int cmd;

    /**
     * Machine readable error code, one of the {@code CODE_*} constants or a business defined value.
     */
    private int errorCode;

    /**
     * Human readable error message, never {@code null}.
     */
    private String message;

    public FailureResponse() {
    }

    public FailureResponse(int index, int cmd, int errorCode, String message) {
        this.index = index;
        this.cmd = cmd;
        this.errorCode = errorCode;
        this.message = message == null ? "" : message;
    }

    /**
     * Registers the framework failure message into the given message factory.
     * Safe to call repeatedly; a duplicate registration of the same class is ignored.
     *
     * @param messageFactory message factory used by the codec layer
     */
    public static void registerTo(MessageFactory messageFactory) {
        if (messageFactory.getMessage(FAILURE_CMD) == null) {
            messageFactory.registerMessage(FAILURE_CMD, FailureResponse.class);
        }
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getCmd() {
        return cmd;
    }

    public void setCmd(int cmd) {
        this.cmd = cmd;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "FailureResponse{index=" + index + ", cmd=" + cmd
                + ", errorCode=" + errorCode + ", message='" + message + "'}";
    }
}
