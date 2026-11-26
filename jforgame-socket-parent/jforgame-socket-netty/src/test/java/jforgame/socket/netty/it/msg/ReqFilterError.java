package jforgame.socket.netty.it.msg;

import jforgame.socket.core.protocol.annotation.MessageMeta;

/**
 * Request that makes the IO filter chain throw before it ever reaches business logic.
 */
@MessageMeta(cmd = 1004)
public class ReqFilterError {

    private int index;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
