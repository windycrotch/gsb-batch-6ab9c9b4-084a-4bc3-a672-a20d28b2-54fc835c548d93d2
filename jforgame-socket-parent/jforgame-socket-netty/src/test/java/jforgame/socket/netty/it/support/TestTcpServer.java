package jforgame.socket.netty.it.support;

import jforgame.codec.json.JsonCodec;
import jforgame.socket.core.protocol.message.MessageFactory;
import jforgame.socket.netty.server.TcpSocketServer;
import jforgame.socket.netty.server.TcpSocketServerBuilder;
import jforgame.threadmodel.dispatch.DispatchThreadModel;

/**
 * Boots a real Netty TCP server (real length-prefixed encoder/decoder + ChannelIoHandler)
 * backed by the test dispatcher and a real dispatch thread model.
 */
public class TestTcpServer {

    private final int port;
    private final TcpSocketServer server;
    private final TestMessageDispatcher dispatcher;
    private final DispatchThreadModel threadModel;
    private final MessageFactory messageFactory;

    public TestTcpServer(int port) {
        this.port = port;
        this.messageFactory = new TestMessageFactory();
        // Small worker count is enough and keeps interleaving tests deterministic enough.
        this.threadModel = new DispatchThreadModel(4);
        this.dispatcher = new TestMessageDispatcher(messageFactory, threadModel);
        this.server = TcpSocketServerBuilder.newBuilder()
                .bindingPort(jforgame.socket.core.net.HostAndPort.valueOf(port))
                .setMessageFactory(messageFactory)
                .setMessageCodec(new JsonCodec())
                .setSocketIoDispatcher(dispatcher)
                .build();
    }

    public void start() throws Exception {
        server.start();
    }

    public void shutdown() throws Exception {
        server.shutdown();
    }

    public int getPort() {
        return port;
    }

    public TestMessageDispatcher getDispatcher() {
        return dispatcher;
    }

    public MessageFactory getMessageFactory() {
        return messageFactory;
    }

    public DispatchThreadModel getThreadModel() {
        return threadModel;
    }
}
