package webcrawler;

import webcrawler.crawler.DomParserBolt;
import webcrawler.xpathengine.*;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class TestXPathEngine {

    String[] XPaths = new String[] {
            "/dwml/head/product/title",
            "/rss/channel/copyright[contains(text(),\"2006\")]",
            "/html/body/ul/li/a[contains(text(),\"weather\")]"};

    XPathEngineImpl engine = new XPathEngineImpl();
    XPathParser parser = new XPathParser(XPaths);
    ArrayList<ArrayList<PathNode>> nodesList = parser.getNodesList();

    @Test
    public void testBuildQueryIndex() {
        XPathMatcher matcher = new XPathMatcher(nodesList);
        HashMap<String, HashMap<String, ArrayList<PathNode>>> QueryIndex = matcher.getQueryIndex();
        assert QueryIndex.containsKey("dwml");
        assert QueryIndex.containsKey("head");
        assert QueryIndex.containsKey("product");
        assert QueryIndex.containsKey("title");
        assert QueryIndex.containsKey("rss");
        assert QueryIndex.containsKey("channel");
        assert QueryIndex.containsKey("copyright");
        assert QueryIndex.containsKey("html");
        assert QueryIndex.containsKey("body");
        assert QueryIndex.containsKey("ul");
        assert QueryIndex.containsKey("li");
        assert QueryIndex.containsKey("a");
    }

    @Test
    public void testEngineRun() throws IOException {
        Path fileName = Path.of("./src/test/test.xml");
        String xmlFile = Files.readString(fileName);
        String url = "http://test.xml";
        Fields schema = new Fields("url", "file", "done");
        List<Object> objects = new LinkedList<>();
        objects.add(url);
        objects.add(xmlFile);
        objects.add(false);
        Tuple input = new Tuple(schema, objects);

        DomParserBolt parser = new DomParserBolt();
        parser.execute(input);
        ArrayList<OccurrenceEvent> eventLists = parser.getEventList();

        XPathMatcher matcher = new XPathMatcher(nodesList);
        engine.setXPathMatcher(matcher);
        engine.setXPaths(XPaths);

        for (OccurrenceEvent event: eventLists) {
            engine.evaluateEvent(event);
        }

        assert matcher.getBitmap()[1];
    }
}
