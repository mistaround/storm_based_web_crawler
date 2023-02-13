package webcrawler.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class ChannelData implements Serializable {
    public static final long serialVersionUID = -3043453123456271695L;

    private final String name;
    private final String author;
    private final String Xpath;
    private ArrayList<String> docUrls = new ArrayList<>();

    public ChannelData(String name, String author, String Xpath) {
        this.name = name;
        this.author = author;
        this.Xpath = Xpath;
    }

    public void addDocUrl(String url) {
        if (!docUrls.contains(url))
            docUrls.add(url);
    }

    public void replaceDocUrls(ArrayList<String> urls) {
        docUrls = urls;
    }

    public String getName() {
        return name;
    }

    public String getAuthor() {
        return author;
    }

    public String getXpath() {
        return Xpath;
    }

    public ArrayList<String>  getDocUrls() {return docUrls;}

    public String toString() {
        return "[ChannelData: name=" + name + " created by: " + author + " with documents: " + getDocUrls().toString() +  "]";
    }
}
