package webcrawler.crawler;

import webcrawler.crawler.utils.URLInfo;
import stormlite.OutputFieldsDeclarer;
import stormlite.TopologyContext;
import stormlite.routers.IStreamRouter;
import stormlite.spout.IRichSpout;
import stormlite.spout.SpoutOutputCollector;
import stormlite.tuple.Fields;
import stormlite.tuple.Values;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static webcrawler.crawler.utils.NetUtils.getRobots;

public class QueueSpout implements IRichSpout {
    static Logger logger = LogManager.getLogger(QueueSpout.class);

    private final String executorId = UUID.randomUUID().toString();
    private SpoutOutputCollector collector;


    static LinkedBlockingQueue<String> urlQueue = new LinkedBlockingQueue<>();
    static AtomicBoolean isDone = new AtomicBoolean(false);
    final private Map<String, List<Long>> delayTable = new HashMap<>();

    public synchronized void updateDelay(String hostname, int delay) {
        List<Long> list = new ArrayList<>(2);
        list.add(new Date().getTime());
        list.add((long) delay);
        delayTable.put(hostname, list);
    }

    public synchronized List<Long> getDelay(String hostname) {
        return delayTable.getOrDefault(hostname, null);
    }

    public boolean isDelaying(List<Long> list) {
        if (list == null) {
            return false;
        }
        if((new Date().getTime()) - list.get(0) < list.get(1) * 1000) {
            return true;
        }
        return false;
    }

    /**
     * Called when a task for this component is initialized within a
     * worker on the cluster. It provides the spout with the environment
     * in which the spout executes.
     *
     * @param config    The Storm configuration for this spout. This is
     *                  the configuration provided to the topology merged in
     *                  with cluster configuration on this machine.
     * @param topo
     * @param collector The collector is used to emit tuples from
     *                  this spout. Tuples can be emitted at any time, including
     *                  the open and close methods. The collector is thread-safe
     *                  and should be saved as an instance variable of this spout
     */
    @Override
    public void open(Map<String, Object> config, TopologyContext topo, SpoutOutputCollector collector) {
        this.collector = collector;
        urlQueue.add((String) config.getOrDefault("startUrl", null));
        logger.info("Open CrawlerQueueSpout " + executorId);
    }

    /**
     * Called when an ISpout is going to be shutdown.
     * There is no guarantee that close will be called, because the
     * supervisor kill -9’s worker processes on the cluster.
     */
    @Override
    public void close() {
        logger.info("Close CrawlerQueueSpout " + executorId);
    }

    /**
     * When this method is called, Storm is requesting that the Spout emit
     * tuples to the output collector. This method should be non-blocking,
     * so if the Spout has no tuples to emit, this method should return.
     */
    @Override
    public void nextTuple() {
        if (isDone.get()) {
            urlQueue.poll();
            return;
        }
        try {
            if (!urlQueue.isEmpty()) {
                String url = urlQueue.poll();
                URLInfo info = new URLInfo(url);
                List<String> Disallow;

                if (isDelaying(getDelay(info.getHostName()))) {
                    urlQueue.add(url);
                    return;
                }
                logger.info("Incoming URL: " + url + " Trying to find robots.txt");
                Map<String, List<String>> robots = getRobots(url);
                // Get robots.txt
                if (robots != null) {
                    Disallow = robots.getOrDefault("Disallow", null);
                    int crawlDelay = Integer.parseInt(robots.get("Crawl-delay").get(0));
                    String hostname = info.getHostName();
                    updateDelay(hostname, crawlDelay);
                } else {
                    logger.info("No Robots.txt found, start crawling");
                    this.collector.emit(new Values<>(url));
                    return;
                }

                // Match Disallow
                boolean allowed = true;
                if (Disallow != null) {
                    for (String disallow : Disallow) {
                        if (disallow.endsWith("/")) {
                            disallow = disallow.substring(0, disallow.length() - 2);
                        }
                        if (url.contains(disallow)) {
                            logger.info("Not allowed to crawl on: " + url);
                            allowed = false;
                            break;
                        }
                    }
                }
                if (allowed) {
                    logger.info("Allowed, start crawling: " + url);
                    this.collector.emit(new Values<>(url));
                } else {
                    logger.info("Not allowed, discard: " + url);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setRouter(IStreamRouter router) {
        this.collector.setRouter(router);
    }

    @Override
    public String getExecutorId() {
        return executorId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url"));
    }

    public boolean isQueueEmpty() {
        if (urlQueue != null) {
            return urlQueue.isEmpty();
        }
        return true;
    }

}