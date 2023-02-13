package webcrawler.crawler;

import webcrawler.xpathengine.OccurrenceEvent;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.bolt.IRichBolt;
import stormlite.bolt.OutputCollector;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import stormlite.tuple.Values;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Node;
import org.jsoup.parser.Parser;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

public class DomParserBolt implements IRichBolt {
    static Logger logger = LogManager.getLogger(DomParserBolt.class);

    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector = null;
    Fields schema = new Fields("event", "url", "EOF");
    ArrayList<OccurrenceEvent> eventList = new ArrayList<>();

    public ArrayList<OccurrenceEvent> getEventList() {
        return eventList;
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
        boolean done = (boolean) input.getObjectByField("done");
        if (done) return;

        String url = input.getStringByField("url");
        String file = input.getStringByField("file");

        if (!url.equals("null") && file != null) {
            logger.info("Parsing document DOM of: " + url);
            Document doc;
            if (url.endsWith(".xml")) {
                doc = Jsoup.parse(file, "", Parser.xmlParser());
            } else {
                doc = Jsoup.parse(file, "", Parser.htmlParser());
            }

            Node root = doc.root();
            NodeVisitor visitor = new NodeVisitor() {
                int level = 0;
                @Override
                public void head(Node node, int i) {
                    OccurrenceEvent event;
                    if (node.nodeName().equals("#text")) {
                        event = new OccurrenceEvent(OccurrenceEvent.Type.Text, node.toString(), url, level);
                        event.setParent(node.parentNode().nodeName());
                    } else {
                        if (!node.nodeName().startsWith("#")) level++;
                        event = new OccurrenceEvent(OccurrenceEvent.Type.Open, node.nodeName(), url, level);
                    }
                    if (collector != null)
                        collector.emit(new Values<>(event, url, false));
                    eventList.add(event);
                }

                @Override
                public void tail(Node node, int i) {
                    OccurrenceEvent event;
                    if (!node.nodeName().equals("#text")) {
                        event = new OccurrenceEvent(OccurrenceEvent.Type.Close, node.nodeName(), url, level);
                        if (!node.nodeName().startsWith("#")) {
                            level --;
                            if (collector != null)
                                collector.emit(new Values<>(event, url, false));
                        } else {
                            if (node.nodeName().startsWith("#doc"))
                                if (collector != null)
                                    collector.emit(new Values<>(event, url, true));
                        }
                        eventList.add(event);
                    }
                }
            };

            NodeTraversor traversor = new NodeTraversor(visitor);
            traversor.traverse(root);

            logger.info("End parsing: " + url);
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
        this.collector = collector;
    }

    /**
     * Called during topology creation: sets the output router
     *
     * @param router
     */
    @Override
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

    /**
     * Get the list of fields in the stream tuple
     *
     * @return
     */
    @Override
    public Fields getSchema() {
        return schema;
    }

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

}
