package webcrawler;

import webcrawler.xpathengine.PathNode;
import webcrawler.xpathengine.XPathParser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class XPathParserTest {
    String[] XPaths = new String[] {
            "/dwml/head/product/title",
            "/rss/channel/copyright[contains(text(),\"2006\")]",
            "/rss/channel/item/author[text()=\"STEVEN ERLANGER\")]",
            "/html/body/ul/li/a[contains(text(),\"weather\")]",
            "/dvf/[cfev/+3c]"};

    @Test
    public void testXPathParser() {
        XPathParser xPathParser = new XPathParser(XPaths);
        ArrayList<ArrayList<PathNode>> nodesList = xPathParser.getNodesList();

        System.out.println(Arrays.toString(nodesList.toArray()));

        assert nodesList.size() == 4;
        assert nodesList.get(0).size() == 4;
        assert nodesList.get(1).size() == 3;
        assert nodesList.get(2).size() == 4;
        assert nodesList.get(3).size() == 5;
    }

    @Test
    public void testInvalidXPath() {
        XPathParser xPathParser = new XPathParser(XPaths);
        boolean[] bitmap = xPathParser.getBitmap();

        assert bitmap[0];
        assert bitmap[1];
        assert bitmap[2];
        assert bitmap[3];
        assert !bitmap[4];
    }
}
