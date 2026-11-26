package jforgame.socket.netty.it.support;

import jforgame.codec.json.JsonCodec;
import jforgame.socket.core.protocol.message.DefaultMessageHeader;
import jforgame.socket.core.protocol.message.FailureResponse;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Minimal raw TCP test client. It writes and reads the exact 12-byte header + body wire format,
 * forcing the server's real Netty {@code DefaultProtocolDecoder}/{@code DefaultProtocolEncoder}
 * to do the framing and JSON codec work. No framework client classes are used on this side,
 * which keeps the assertions independent of the production dispatch path.
 */
public class RawTcpClient implements AutoCloseable {

    private final Socket socket;
    private final DataInputStream in;
    private final TestMessageFactory messageFactory;
    private final JsonCodec codec = new JsonCodec();

    public RawTcpClient(String host, int port, TestMessageFactory messageFactory) throws IOException {
        this.messageFactory = messageFactory;
        this.socket = new Socket();
        this.socket.connect(new InetSocketAddress(host, port), 5000);
        this.socket.setSoTimeout(5000);
        this.in = new DataInputStream(socket.getInputStream());
    }

    /**
     * Sends one request frame.
     */
    public void sendRequest(int index, int cmd, Object message) throws IOException {
        byte[] body = codec.encode(message);
        int msgLength = DefaultMessageHeader.SIZE + body.length;
        ByteBuffer buffer = ByteBuffer.allocate(msgLength);
        buffer.putInt(msgLength);
        buffer.putInt(index);
        buffer.putInt(cmd);
        buffer.put(body);
        socket.getOutputStream().write(buffer.array());
        socket.getOutputStream().flush();
    }

    /**
     * Reads one full frame (blocking up to the socket timeout).
     */
    public Frame readFrame() throws IOException {
        byte[] headerBytes = new byte[DefaultMessageHeader.SIZE];
        readFully(headerBytes);
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerBytes);
        int msgLength = headerBuffer.getInt();
        int index = headerBuffer.getInt();
        int cmd = headerBuffer.getInt();
        int bodySize = msgLength - DefaultMessageHeader.SIZE;
        byte[] body = new byte[bodySize];
        readFully(body);
        Class<?> clazz = messageFactory.getMessage(cmd);
        Object message = codec.decode(clazz, body);
        return new Frame(msgLength, index, cmd, body, message);
    }

    /**
     * Reads a frame, returning null on timeout instead of failing (used when no response is expected).
     */
    public Frame readFrameOrNull() {
        try {
            return readFrame();
        } catch (IOException e) {
            return null;
        }
    }

    private void readFully(byte[] target) throws IOException {
        in.readFully(target);
    }

    public boolean isConnected() {
        return socket.isConnected() && !socket.isClosed();
    }

    public Socket rawSocket() {
        return socket;
    }

    /**
     * Decodes the body of an already-read frame as the given type.
     */
    public <T> T decode(Frame frame, Class<T> type) {
        return type.cast(codec.decode(type, frame.body));
    }

    /**
     * Convenience accessor for tests asserting on failure receipts.
     */
    public FailureResponse decodeFailure(Frame frame) {
        return decode(frame, FailureResponse.class);
    }

    @Override
    public void close() throws IOException {
        try {
            socket.close();
        } finally {
            in.close();
        }
    }

    /** One decoded wire frame. */
    public static class Frame {
        public final int msgLength;
        public final int index;
        public final int cmd;
        public final byte[] body;
        public final Object message;

        Frame(int msgLength, int index, int cmd, byte[] body, Object message) {
            this.msgLength = msgLength;
            this.index = index;
            this.cmd = cmd;
            this.body = body;
            this.message = message;
        }

        public String bodyUtf8() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }
}
