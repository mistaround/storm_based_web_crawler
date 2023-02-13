package webcrawler.crawler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import webcrawler.crawler.utils.URLInfo;
import webcrawler.storage.StorageFactory;
import webcrawler.storage.StorageInterface;
import stormlite.Config;
import stormlite.LocalCluster;
import stormlite.Topology;
import stormlite.TopologyBuilder;
import stormlite.tuple.Fields;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Crawler implements CrawlMaster {

    static Logger logger = LogManager.getLogger(Crawler.class);

    private static final String CRAWLER_QUEUE_SPOUT = "CRAWLER_QUEUE_SPOUT";
    private static final String DOC_FETCHER_BOLT = "DOC_FETCHER_BOLT";
    private static final String LINK_EXTRACTOR_BOLT = "LINK_EXTRACTOR_BOLT";
    private static final String DOM_PARSER_BOLT = "DOM_PARSER_BOLT";
    private static final String PATH_MATCHER_BOLT = "PATH_MATCHER_BOLT";
    private static final int TIMEOUT = 100;

    private final StorageInterface db;
    private final QueueSpout queueSpout;
    private final LocalCluster cluster;
    private int idleCount = 0;
    private int lastCheck = 0;

    public Crawler(StorageInterface db, QueueSpout queueSpout, LocalCluster cluster) {
        this.db = db;
        this.queueSpout = queueSpout;
        this.cluster = cluster;
    }

    public void start() {
        logger.info("STARTING CRAWLER APP");
    }

    public void shutdown() {
        logger.info("SHUTDOWN CRAWLER APP");
        this.db.close();
    }

    /**
     * We've indexed another document
     */
    @Override
    public void incCount() {}

    /**
     * Workers can poll this to see if they should exit, ie the crawl is done
     */
    @Override
    public boolean isDone() {
        if (QueueSpout.isDone.get()) {
            return true;
        }
//        if (idleCount > TIMEOUT) {
//            return true;
//        }
//        if (db.getCorpusSize() == lastCheck) {
//            idleCount ++;
//        } else {
//            lastCheck = db.getCorpusSize();
//            idleCount = 0;
//        }
//        return false;
        return queueSpout.isQueueEmpty() && !cluster.isActive();
    }

    /**
     * Workers should notify when they are processing an URL
     *
     * @param working
     */
    @Override
    public void setWorking(boolean working) {}

    /**
     * Workers should call this when they exit, so the master knows when it can shut
     * down
     */
    @Override
    public void notifyThreadExited() {}

    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("webcrawler", Level.DEBUG);

        if (args.length < 3 || args.length > 5) {
            System.out.println("Usage: Crawler {start URL} {database environment path} {max doc size in MB} {number of files to index}");
            System.exit(1);
        }

        String startUrl = args[0];
        String envPath = args[1];
        int size = Integer.parseInt(args[2]);
        int count = args.length == 4 ? Integer.parseInt(args[3]) : 100;

        if (!Files.exists(Paths.get(envPath))) {
            try {
                Files.createDirectory(Paths.get(envPath));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        StorageInterface db = StorageFactory.getDatabaseInstance(envPath);
        startUrl = new URLInfo(startUrl).toString();

        Config config = new Config();
        config.put("startUrl", startUrl);
        config.put("envPath", envPath);
        config.put("size", size);
        config.put("count", count);
        config.put("db", db);

        QueueSpout queueSpout = new QueueSpout();
        DocFetcherBolt docBolt = new DocFetcherBolt();
        LinkExtractorBolt linkBolt = new LinkExtractorBolt();
        DomParserBolt domBolt = new DomParserBolt();
        PathMatcherBolt pathBolt = new PathMatcherBolt();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(CRAWLER_QUEUE_SPOUT, queueSpout, 1);
        builder.setBolt(DOC_FETCHER_BOLT, docBolt, 4).shuffleGrouping(CRAWLER_QUEUE_SPOUT);
        builder.setBolt(LINK_EXTRACTOR_BOLT, linkBolt, 4).shuffleGrouping(DOC_FETCHER_BOLT);
        builder.setBolt(DOM_PARSER_BOLT, domBolt, 4).fieldsGrouping(DOC_FETCHER_BOLT, new Fields("url"));
        builder.setBolt(PATH_MATCHER_BOLT, pathBolt, 1).fieldsGrouping(DOM_PARSER_BOLT, new Fields("url"));

        LocalCluster cluster = new LocalCluster();
        Topology topo = builder.createTopology();
        ObjectMapper mapper = new ObjectMapper();
        Crawler crawler = new Crawler(db, queueSpout, cluster);

        try {
            String str = mapper.writeValueAsString(topo);
            System.out.println("The StormLite topology is:\n" + str);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        System.out.println("Starting crawl of " + count + " documents, starting at " + startUrl);
        crawler.start();
        cluster.submitTopology("crawler", config, builder.createTopology());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while (!crawler.isDone()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Done crawling!");
        cluster.killTopology("crawler");
        cluster.shutdown();
        crawler.shutdown();
    }
}
