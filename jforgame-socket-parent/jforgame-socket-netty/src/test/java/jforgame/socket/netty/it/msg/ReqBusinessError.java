package jforgame.socket.netty.it.msg;

import jforgame.socket.core.protocol.annotation.MessageMeta;

/**
 * Request whose handler always throws a business exception.
 */
@MessageMeta(cmd = 1003)
public class ReqBusinessError {

    private int index;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
