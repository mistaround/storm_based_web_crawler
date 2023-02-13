package webcrawler.xpathengine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class XPathEngineImpl implements XPathEngine {
    static Logger logger = LogManager.getLogger(XPathEngineImpl.class);

    String[] XPaths;
    XPathMatcher xPathMatcher;

    /**
     * Sets the XPath expression(s) that are to be evaluated.
     *
     * @param expressions
     */
    @Override
    public void setXPaths(String[] expressions) {
        this.XPaths = expressions;
    }

    public void setXPathMatcher(XPathMatcher xPathMatcher) {
        this.xPathMatcher = xPathMatcher;
    }

    /**
     * Event driven pattern match.
     * <p>
     * Takes an event at a time as input
     *
     * @param event notification about something parsed, from a given document
     * @return bit vector of matches to XPaths
     */
    @Override
    public boolean[] evaluateEvent(OccurrenceEvent event) {
//        logger.debug(event.toString());
        if (event.type == OccurrenceEvent.Type.Open) {
            xPathMatcher.handleOpen(event);
        }
        if (event.type == OccurrenceEvent.Type.Close) {
            xPathMatcher.handleClose(event);
        }
        if (event.type == OccurrenceEvent.Type.Text) {
            xPathMatcher.handleText(event);
        }

        return null;
    }
}
