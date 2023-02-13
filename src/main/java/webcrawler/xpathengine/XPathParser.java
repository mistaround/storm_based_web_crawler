package webcrawler.xpathengine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.regex.Pattern;

public class XPathParser {
    static Logger logger = LogManager.getLogger(XPathParser.class);
    private final ArrayList<ArrayList<PathNode>> nodesList;
    private final String[] Xpaths;
    private final boolean[] bitmap;

    public XPathParser(String[] Xpaths) {
        this.nodesList = new ArrayList<>();
        this.Xpaths = Xpaths;
        this.bitmap = new boolean[Xpaths.length];
        this.runParser();
    }

    public void runParser() {
        int i = 0;
        int queryId1 = 1;
        for (String Xpath: Xpaths) {
            String[] queries = Xpath.substring(1).split("/");
            ArrayList<PathNode> nodes = new ArrayList<>(queries.length);
            boolean valid = true;
            int queryId2 = 1;
            for (String query: queries) {
                int p = query.indexOf('[');
                if (p == -1) {
                    // /foo
                    if (!isValidNodeName(query.trim())) {
                        logger.info("INVALID NODENAME: " + query);
                        valid = false;
                        break;
                    }
                    String nodeName = query.trim();
                    nodes.add(new PathNode(queryId1, queryId2, queries.length, nodeName));
                } else {
                    // /abc[contains(text(),"someSubstring")]
                    // /c[text() = "whiteSpacesShouldNotMatter"]
                    String nodeName = query.substring(0, p).trim();
                    String filter = query.substring(p).trim();
                    if (!isValidFilter(filter)) {
                        logger.info("INVALID FILTER: " + nodeName + " " + filter);
                        valid = false;
                        break;
                    }
                    PathNode node = new PathNode(queryId1, queryId2, queries.length, nodeName);
                    node.setFilter(filter.substring(1,filter.length()-1));
                    nodes.add(node);
                }
                queryId2 ++;
            }
            if (valid) {
                bitmap[i] = true;
                nodesList.add(nodes);
                queryId1++;
            } else {
                bitmap[i] = false;
            }
            i ++;
        }
    }

    private boolean isValidNodeName(String nodeName) {
        Pattern nameRegex = Pattern.compile("^(?!(?:[Xx][Mm][Ll]))[a-zA-Z0-9\\-_\\.]+$");
        return nameRegex.matcher(nodeName).find();
    }

    private boolean isValidFilter(String filter) {
        // [contains(text(),"someSubstring")]
        // [text() = "whiteSpacesShouldNotMatter"]
        if (!filter.endsWith("]") )
            return false;
        filter = filter.substring(1,filter.length()-1);
        Pattern textRegex = Pattern.compile("text\\s*\\(\\s*\\)\\s*=\\s*\\\"(.*)\\\"\\s*");
        Pattern containRegex = Pattern.compile("contains\\s*\\(text\\s*\\(\\s*\\)\\s*\\,\\s*\\\"(.*)\\s*\\\"\\s*\\)");
        return textRegex.matcher(filter).find() || containRegex.matcher(filter).find();
    }

    public ArrayList<ArrayList<PathNode>> getNodesList() {
        return this.nodesList;
    }

    public boolean[] getBitmap() {
        return this.bitmap;
    }

}
