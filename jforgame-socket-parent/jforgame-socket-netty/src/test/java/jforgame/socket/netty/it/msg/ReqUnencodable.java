package jforgame.socket.netty.it.msg;

import jforgame.socket.core.protocol.annotation.MessageMeta;

/**
 * A request whose response cannot be serialized, used to prove that a failed failure/response
 * send does not break processing of subsequent requests.
 */
@MessageMeta(cmd = 1005)
public class ReqUnencodable {

    private int index;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
