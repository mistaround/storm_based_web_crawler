package webcrawler.crawler;

import webcrawler.storage.StorageManager;
import webcrawler.xpathengine.*;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.bolt.IRichBolt;
import stormlite.bolt.OutputCollector;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class PathMatcherBolt implements IRichBolt {
    static Logger logger = LogManager.getLogger(PathMatcherBolt.class);

    String executorId = UUID.randomUUID().toString();
    ArrayList<ArrayList<PathNode>> pathNodesList;
    HashMap<String, XPathMatcher> matchers;
    boolean[] validMap;
    boolean[] matchMap;
    String[] channels;
    String[] XPaths;
    StorageManager db;

    public boolean[] getMatchMap() {
        return this.matchMap;
    }

    /**
     * Called when a bolt is about to be shut down
     */
    @Override
    public void cleanup() {

    }

    /**
     * Processes a tuple
     *
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        String url = input.getStringByField("url");
        boolean EOF = (boolean) input.getObjectByField("EOF");
        OccurrenceEvent event = (OccurrenceEvent) input.getObjectByField("event");

        XPathEngineImpl engine = (XPathEngineImpl) XPathEngineFactory.getXPathEngine();
        XPathMatcher matcher = matchers.getOrDefault(url, null);
        if (matcher == null) {
            matcher = new XPathMatcher(pathNodesList);
            matchers.put(url, matcher);
        }
        engine.setXPaths(XPaths);
        engine.setXPathMatcher(matcher);
        engine.evaluateEvent(event);

        if (EOF) {
            matchMap = matcher.getBitmap();
            for (int i = 0; i < matchMap.length; i++) {
                matchMap[i] &= validMap[i];
                if (matchMap[i] && db != null && channels != null) {
                    db.updateChannel(channels[i], url);
                }
            }
            logger.info("--------------- RESULT " + url + " ------------------");
            logger.info("--------------------- " + Arrays.toString(matchMap) + " -----------------------");
        }
    }

    /**
     * Called when this task is initialized
     *
     * @param stormConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.db = (StorageManager) stormConf.getOrDefault("db", null);
        if (db !=null) {
            this.channels = db.getChannelsXpaths().getOrDefault("channels", null);
            this.XPaths = db.getChannelsXpaths().get("Xpaths");
        } else {
            this.XPaths = (String[]) stormConf.get("Xpaths");
        }

        this.matchers = new HashMap<>();

        XPathParser xPathParser = new XPathParser(this.XPaths);
        this.validMap = xPathParser.getBitmap();
        this.pathNodesList = xPathParser.getNodesList();

        pathNodesList.forEach((nodes) -> {
            nodes.forEach((node) -> {
                logger.info(node.toString());
            });
        });
    }

    /**
     * Called during topology creation: sets the output router
     *
     * @param router
     */
    @Override
    public void setRouter(IStreamRouter router) {
    }

    /**
     * Get the list of fields in the stream tuple
     *
     * @return
     */
    @Override
    public Fields getSchema() {
        return new Fields();
    }

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }

}
