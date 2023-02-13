package webcrawler.crawler;

import webcrawler.crawler.utils.NetUtils;
import webcrawler.storage.StorageManager;
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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DocFetcherBolt implements IRichBolt {
    static Logger logger = LogManager.getLogger(DocFetcherBolt.class);

    String executorId = UUID.randomUUID().toString();
    private OutputCollector collector;
    StorageManager db;
    int maxSize;
    int maxCount;
    Fields schema = new Fields("url", "file", "done");
    static AtomicInteger docCount = new AtomicInteger(0);

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
        if (docCount.get() == maxCount) {
            collector.emit(new Values<>("null", null, true));
            return;
        }

        String workUrl = input.getStringByField("url");
        // Start crawling
        Map<String, List<String>> header = NetUtils.getHeader(workUrl);
        if (isValid(header)) {
            String file;
            boolean existed = false;
            if (db.getDocument(workUrl) != null && !isModified(header, workUrl)) {
                logger.info("Already in database and not modified since");
                file = db.getDocument(workUrl);
                existed = true;
                logger.info("Fetched");
            } else {
                file = NetUtils.getFile(workUrl);
                logger.info("Downloaded");
            }

            if (docCount.get() == maxCount) {
                collector.emit(new Values<>("null", null, true));
                return;
            }

            if (!existed) {
                if (!hasSeen(file)) {
                    logger.info("Adding to database");
                    List<String> type = header.getOrDefault("Content-Type", null);

                    if (docCount.get() == maxCount) {
                        collector.emit(new Values<>("null", null, true));
                        return;
                    }
                    if (type != null) {
                        db.addDocument(workUrl, file, type.get(0));
                    } else {
                        db.addDocument(workUrl, file, "text/html");
                    }

                    collector.emit(new Values<>(workUrl, file, false));
                    incCount();
                    logger.info("Added");
                }
            } else {
                logger.info("File content duplicated");
            }
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
        this.maxSize = (int) stormConf.get("size");
        this.maxCount = (int) stormConf.get("count");
        this.db = (StorageManager) stormConf.get("db");
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

    private boolean isValid(Map<String, List<String>> header) {
        if (header == null) {
            return false;
        }
        if (header.containsKey("Content-Length")) {
            int bytes = Integer.parseInt(header.get("Content-Length").get(0));
            logger.info("Incoming File of size: " + bytes);
            if (bytes > maxSize * 1024 * 1024) {
                logger.info("File too large");
                return false;
            }
        } else {
            logger.info("No Content-Length field");
            return false;
        }
        if (header.containsKey("Content-Type")) {
            String type = header.get("Content-Type").get(0);
            logger.info("Incoming File of type: " + type);
            if (!type.contains("text/html") && !type.endsWith("xml")) {
                logger.info("File type rejected");
                return false;
            }
        } else {
            return false;
        }
        return true;
    }

    private boolean isModified(Map<String, List<String>> header, String url) {
        if (header.containsKey("Last-Modified")) {
            String date = header.get("Last-Modified").get(0).trim();
            DateFormat df = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
            df.setTimeZone(TimeZone.getTimeZone("GMT"));
            try {
                Date web = df.parse(date);
                return db.isModified(url, web.getTime());
            } catch (ParseException e) {
                logger.debug("NO NO NO");
            }
        }
        return false;
    }

    private boolean hasSeen(String content) {
        String md5 = StorageManager.encode(content, "MD5");
        return db.alreadyExist(md5);
    }

    private void incCount() {
        logger.info("Doc No." + docCount.addAndGet(1) + " indexed");
    }

}

