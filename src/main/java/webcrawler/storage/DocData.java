package webcrawler.storage;

import java.io.Serializable;

public class DocData implements Serializable {
    public static final long serialVersionUID = -3040096123456271695L;
    private final String content;
    private final String type;
    private final long lastModified;

    public DocData(String content, long lastModified, String type) {
        this.content = content;
        this.lastModified = lastModified;
        this.type = type;
    }

    public String getContent() {
        return content;
    }

    public long getLastModified() {
        return lastModified;
    }

    public String getType() {return type;}

    public String toString() {
        return "[DocData: content=" + content + ']';
    }
}
