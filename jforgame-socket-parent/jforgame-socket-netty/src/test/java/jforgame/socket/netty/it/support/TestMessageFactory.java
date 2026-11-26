package jforgame.socket.netty.it.support;

import jforgame.socket.core.protocol.message.DefaultMessageFactory;
import jforgame.socket.core.protocol.message.FailureResponse;
import jforgame.socket.netty.it.msg.ReqBusinessError;
import jforgame.socket.netty.it.msg.ReqEcho;
import jforgame.socket.netty.it.msg.ReqFilterError;
import jforgame.socket.netty.it.msg.ReqUnencodable;
import jforgame.socket.netty.it.msg.ResEcho;
import jforgame.socket.netty.it.msg.ResUnencodable;

/**
 * Message factory used by the integration tests. Registers every test message plus the
 * framework {@link FailureResponse} so failure receipts can travel the real encoder.
 */
public class TestMessageFactory extends DefaultMessageFactory {

    public TestMessageFactory() {
        registerMessage(1001, ReqEcho.class);
        registerMessage(1002, ResEcho.class);
        registerMessage(1003, ReqBusinessError.class);
        registerMessage(1004, ReqFilterError.class);
        registerMessage(1005, ReqUnencodable.class);
        registerMessage(1006, ResUnencodable.class);
        FailureResponse.registerTo(this);
    }
}
