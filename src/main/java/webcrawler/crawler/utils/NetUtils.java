package webcrawler.crawler.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class NetUtils {

    static final Logger logger = LogManager.getLogger(NetUtils.class);

    static final String USER_AGENT = "cis455crawler";

    static public Map<String, List<String>> getRobots(String curUrl) {
        URLInfo info = new URLInfo(curUrl);
        info.setFilePath("/robots.txt");
        String robotsURL = info.toString();
        String file = getFile(robotsURL);
        if (file == null) {
            return null;
        }
        List<String> Disallow = null;
        List<String> CrawlDelay = new LinkedList<>();
        String delay = "0";
        String[] keyValues = file.split("\n");
        for (int i = 0; i < keyValues.length; i ++) {
            if (keyValues[i].startsWith("User")) {
                String pair = keyValues[i];
                String user = pair.substring(pair.indexOf(":")+1).trim();
                if (user.equals("*")) {
                    List<String> tmp = new LinkedList<>();
                    for (int j = i + 1; j < keyValues.length; j++) {
                        String item = keyValues[j];
                        if (item.contains(":")) {
                            int p = item.indexOf(":");
                            String key = item.substring(0,p);
                            String val = item.substring(p+1).trim();
                            if (key.startsWith("Dis")) {
                                tmp.add(val);
                            }
                            if (key.startsWith("Crawl")) {
                                delay = val;
                            }
                            if (key.startsWith("User")) {
                                break;
                            }
                        }
                    }
                    Disallow = tmp;
                }
                // precise user agent has priority
                if (user.equals(USER_AGENT)) {
                    List<String> tmp = new LinkedList<>();
                    for (int j = i + 1; j < keyValues.length; j++) {
                        String item = keyValues[j];
                        if (item.contains(":")) {
                            int p = item.indexOf(":");
                            String key = item.substring(0,p);
                            String val = item.substring(p+1).trim();
                            if (key.startsWith("Dis")) {
                                tmp.add(val);
                            }
                            if (key.startsWith("Crawl")) {
                                delay = val;
                            }
                            if (key.startsWith("User")) {
                                break;
                            }
                        }
                    }
                    Disallow = tmp;
                    break;
                }
            }
        }
        CrawlDelay.add(delay);
        Map<String, List<String>> robots = new HashMap<>();
        if (Disallow != null) {
            robots.put("Disallow", Disallow);
        }
        robots.put("Crawl-delay", CrawlDelay);
//        logger.info("Robots info:" + robots);
        return robots;
    }

    static public Map<String, List<String>> getHeader(String curUrl) {
        try {
            URLInfo info = new URLInfo(curUrl);
            // Get the file
            URL url = new URL(curUrl);
            Map<String, List<String>> headers;
            if (!info.isSecure()) {
                HttpURLConnection con = (HttpURLConnection)url.openConnection();
                con.setRequestProperty("User-Agent", USER_AGENT);
                con.setRequestMethod("HEAD");
                headers = con.getHeaderFields();
            } else {
                HttpsURLConnection con = (HttpsURLConnection)url.openConnection();
                con.setRequestProperty("User-Agent", USER_AGENT);
                con.setRequestMethod("HEAD");
                headers = con.getHeaderFields();
            }
//            logger.info("Got Header at: " + curUrl);
            return headers;
        } catch (Exception e) {
            logger.info("Cannot HEAD: " + curUrl);
            return null;
        }

    }

    static public String getFile(String curUrl) {
        try {
            URLInfo info = new URLInfo(curUrl);
            StringBuilder file = new StringBuilder();
            // Get the file
            URL url = new URL(curUrl);
            InputStream is;
            if (!info.isSecure()) {
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestProperty("User-Agent", USER_AGENT);
                is = con.getInputStream();
            } else {
                HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
                con.setRequestProperty("User-Agent", USER_AGENT);
                is = con.getInputStream();
            }
            logger.info("Downloading File at: " + curUrl);
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while((line = reader.readLine()) != null) {
                file.append(line).append("\n");
            }
            reader.close();
            return file.toString();
        } catch (Exception e) {
            logger.info("Fail Downloading: " + curUrl);
            return null;
        }
    }

    static public LinkedList<String> getUrls(String file) {
        try {
            LinkedList<String> urls = new LinkedList<>();
            // Parse File
            Document doc = Jsoup.parse(file);
            Elements links = doc.select("a[href]");
            for (Element ele: links) {
                String url = ele.toString();
                int p = url.indexOf("=\"");
                url = url.substring(p+2);
                p = url.indexOf("\">");
                url = url.substring(0,p);
                urls.add(url);
            }
            return urls;
        } catch (Exception e) {
            return null;
        }
    }
}
