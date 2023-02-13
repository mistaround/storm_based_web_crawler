package webcrawler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import webcrawler.crawler.*;
import webcrawler.crawler.utils.URLInfo;
import webcrawler.storage.StorageFactory;
import webcrawler.storage.StorageInterface;
import stormlite.Config;
import stormlite.LocalCluster;
import stormlite.Topology;
import stormlite.TopologyBuilder;
import stormlite.tuple.Fields;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CrawlUnlimitedTest {
    String startUrl = "https://crawltest.upenn.edu/";
    String envPath = "./test1";
    int size = 1;
    int count = 100;

    void deleteDir(File file){
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                if (!Files.isSymbolicLink(f.toPath())) {
                    deleteDir(f);
                }
            }
        }
        file.delete();
    }

    @Test
    public void unlimitedNumberCrawl() {
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
        String CRAWLER_QUEUE_SPOUT = "CRAWLER_QUEUE_SPOUT";
        builder.setSpout(CRAWLER_QUEUE_SPOUT, queueSpout, 1);
        String DOC_FETCHER_BOLT = "DOC_FETCHER_BOLT";
        builder.setBolt(DOC_FETCHER_BOLT, docBolt, 4).shuffleGrouping(CRAWLER_QUEUE_SPOUT);
        String LINK_EXTRACTOR_BOLT = "LINK_EXTRACTOR_BOLT";
        builder.setBolt(LINK_EXTRACTOR_BOLT, linkBolt, 4).shuffleGrouping(DOC_FETCHER_BOLT);
        String DOM_PARSER_BOLT = "DOM_PARSER_BOLT";
        builder.setBolt(DOM_PARSER_BOLT, domBolt, 4).shuffleGrouping(DOC_FETCHER_BOLT);
        String PATH_MATCHER_BOLT = "PATH_MATCHER_BOLT";
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

        while (!crawler.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number: " + db.getCorpusSize());
        assert (db.getCorpusSize() >= 32);

        System.out.println("Done crawling!");
        cluster.killTopology("crawler");
        cluster.shutdown();
        crawler.shutdown();
    }

    @After
    public void tearDown() {
        deleteDir(new File(envPath));
    }

}
