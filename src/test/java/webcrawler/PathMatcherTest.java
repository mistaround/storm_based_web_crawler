package webcrawler;

import webcrawler.crawler.DomParserBolt;
import webcrawler.crawler.PathMatcherBolt;
import webcrawler.xpathengine.OccurrenceEvent;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class PathMatcherTest {
    String[] XPaths = new String[] {
            "/dwml/head/product/title",
            "/rss/channel/copyright[contains(text(),\"2006\")]",
            "/rss/channel/item/author[text()=\"STEVEN ERLANGER\")]",
            "/html/body/ul/li/a[contains(text(),\"weather\")]",
            "/dvf/[cfev/+3c]"};

    @Test
    public void testPathMatcherRun() throws IOException {
        Map<String, Object> stormConf = new HashMap<>();
        stormConf.put("Xpaths", XPaths);

        Path fileName = Path.of("./src/test/test.xml");
        String xmlFile = Files.readString(fileName);
        String url = "http://test.xml";

        Fields schemaDom = new Fields("url", "file", "done");
        List<Object> objectsDom = new LinkedList<>();
        objectsDom.add(url);
        objectsDom.add(xmlFile);
        objectsDom.add(false);
        Tuple inputDom = new Tuple(schemaDom, objectsDom);

        DomParserBolt parser = new DomParserBolt();
        parser.execute(inputDom);
        ArrayList<OccurrenceEvent> eventLists = parser.getEventList();

        Fields schemaPath = new Fields("event", "url", "EOF");
        PathMatcherBolt bolt = new PathMatcherBolt();
        bolt.prepare(stormConf, null, null);

        for (int i = 0; i < eventLists.size(); i++) {
            List<Object> objectsPath = new LinkedList<>();
            objectsPath.add(eventLists.get(i));
            objectsPath.add(url);
            if (i == eventLists.size()-1) {
                objectsPath.add(true);
            } else {
                objectsPath.add(false);
            }
            Tuple inputPath = new Tuple(schemaPath, objectsPath);
            bolt.execute(inputPath);
        }

        assert bolt.getMatchMap()[1];
        assert bolt.getMatchMap()[2];
    }

}
