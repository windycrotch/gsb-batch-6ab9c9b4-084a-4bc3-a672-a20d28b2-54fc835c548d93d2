package jforgame.socket.netty.it.msg;

import jforgame.socket.core.protocol.annotation.MessageMeta;

/**
 * Normal response for {@link ReqEcho}.
 */
@MessageMeta(cmd = 1002)
public class ResEcho {

    private int index;

    private String content;

    public ResEcho() {
    }

    public ResEcho(int index, String content) {
        this.index = index;
        this.content = content;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "ResEcho{index=" + index + ", content='" + content + "'}";
    }
}
