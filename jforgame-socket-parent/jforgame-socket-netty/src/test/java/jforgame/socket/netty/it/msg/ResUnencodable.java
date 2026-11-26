package jforgame.socket.netty.it.msg;

import jforgame.socket.core.protocol.annotation.MessageMeta;

/**
 * Response carrying a non-JSON-serializable field (a raw {@link Object}).
 * The JSON codec throws when encoding it, simulating a failed response/failure send.
 */
@MessageMeta(cmd = 1006)
public class ResUnencodable {

    private int index;

    private Object broken;

    public ResUnencodable() {
    }

    public ResUnencodable(int index, Object broken) {
        this.index = index;
        this.broken = broken;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public Object getBroken() {
        return broken;
    }

    public void setBroken(Object broken) {
        this.broken = broken;
    }
}
