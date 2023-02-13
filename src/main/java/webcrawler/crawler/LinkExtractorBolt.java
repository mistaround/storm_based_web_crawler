package webcrawler.crawler;

import webcrawler.crawler.utils.NetUtils;
import webcrawler.crawler.utils.URLInfo;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.bolt.IRichBolt;
import stormlite.bolt.OutputCollector;
import stormlite.routers.IStreamRouter;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

import static webcrawler.crawler.QueueSpout.isDone;
import static webcrawler.crawler.QueueSpout.urlQueue;

public class LinkExtractorBolt implements IRichBolt {
    static Logger logger = LogManager.getLogger(LinkExtractorBolt.class);

    String executorId = UUID.randomUUID().toString();
    private boolean isIdle = true;
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
        if (done) {
            logger.info("Crawl done due to num limit");
            if (!isDone.get()) {
                isDone.set(true);
            }
            return;
        }
        String workUrl = input.getStringByField("url");
        String file = input.getStringByField("file");
        URLInfo info = new URLInfo(workUrl);

        LinkedList<String> urls = NetUtils.getUrls(file);

        for (String path: urls) {
            if (!path.contains("http") && !path.contains(":")) {
                URLInfo newInfo = new URLInfo(info.toString());
                if (!path.startsWith("/")) {
                    String filepath = info.getFilePath();
                    if (filepath.endsWith(".html") || filepath.endsWith(".xml")) {
                        int i = 0, p = 0;
                        while (i < filepath.length()) {
                            char c = filepath.charAt(i);
                            if (c == '/')
                                p = i;
                            i++;
                        }
                        String url = filepath.substring(0,p+1) + path;
                        newInfo.setFilePath(url);
                    } else {
                        if (!filepath.endsWith("/")) {
                            filepath += "/";
                        }
                        String url = filepath + path;
                        newInfo.setFilePath(url);
                    }
                } else {
                    newInfo.setFilePath(path);
                }
                path = newInfo.toString();
            }
            urlQueue.add(path);
//            logger.info("Adding New Task To Queue: " + path);
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
