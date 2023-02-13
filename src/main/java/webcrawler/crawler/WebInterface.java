package webcrawler.crawler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static spark.Spark.*;
import webcrawler.crawler.handlers.LoginFilter;
import webcrawler.crawler.handlers.LogoutHandler;
import webcrawler.crawler.handlers.RegistrationHandler;
import webcrawler.crawler.utils.URLInfo;
import webcrawler.storage.StorageFactory;
import webcrawler.storage.StorageInterface;
import webcrawler.crawler.handlers.LoginHandler;
import org.apache.logging.log4j.Level;

public class WebInterface {
    public static void main(String args[]) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("webcrawler", Level.DEBUG);
        if (args.length < 1 || args.length > 2) {
            System.out.println("Syntax: WebInterface {path} {root}");
            System.exit(1);
        }

        if (!Files.exists(Paths.get(args[0]))) {
            try {
                Files.createDirectory(Paths.get(args[0]));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        port(45555);
        StorageInterface database = StorageFactory.getDatabaseInstance(args[0]);

        LoginFilter testIfLoggedIn = new LoginFilter(database);

        if (args.length == 2) {
            staticFiles.externalLocation(args[1]);
            staticFileLocation(args[1]);
        }


        before("/*", "*/*", testIfLoggedIn);
        get("/", (req, res) -> {
            if (req.session(false) == null) {
                return "<!DOCTYPE html>\n" +
                        "<html>\n" +
                        "<head>\n" +
                        "    <title>Main Page</title>\n" +
                        "</head>\n" +
                        "<body>\n" +
                        "<h1>Welcome, please login </h1>\n" +
                        "</body>\n" +
                        "</html>";
            }
            String user = req.session(false).attribute("user");
            HashMap<String, ArrayList<String>> maps = database.getSubscriptionsByUser(user);

            StringBuilder body = new StringBuilder("<body>\n" +
                    "<h1>Welcome, " + user + "</h1>\n").append("<h2>Subscribed Channels:</h2>\n");
            for (Map.Entry<String, ArrayList<String>> entry: maps.entrySet()) {
                String channel = entry.getKey();
                String redirect = "/show?channel=" + channel;
                body.append("<h3><a href=").append(redirect).append(">").append(channel).append("</href></h3>\n");
            }

            body.append("<h2>All Channels:</h2>\n");
            String[] allChannels = database.getChannelsXpaths().get("channels");
            for (String channel: allChannels) {
                String redirect = "/show?channel=" + channel;
                body.append("<h3><a href=").append(redirect).append(">").append(channel).append("</href></h3>\n");
            }
            body.append("</body>\n");

            return "<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<head>\n" +
                    "<title>Main Page</title>\n" +
                    body.toString() +
                    "</head>\n" +
                    "</html>";
        });
        post("/register", new RegistrationHandler(database));
        post("/login", new LoginHandler(database));
        get("/logout", new LogoutHandler(database));
        get("/lookup", (req, res) -> {
            String url = req.queryParams("url");
            if (url == null) {
                halt(404, "Not Found");
                return null;
            }
            url = new URLInfo(url).toString();
            String type = database.getType(url);
            String doc = database.getDocument(url);
            res.type(type);
            return doc;
        });
        get("/create/:name", (req, res) -> {
            boolean success = database.updateChannel(req.params("name"), req.queryParams("xpath"),
                    req.session(false).attribute("user"), null);
            if (success) {
                res.status(200);
                return "Create channel: " + req.params("name") + " success";
            } else {
                halt(400, "Bad Request");
                return "Create channel failed";
            }
        });
        get("/show", (req, res) -> {
            String channel = req.queryParams("channel");
            String author = database.getChannelAuthor(channel);
            if (author == null) {
                res.status(404);
                return "No such channel";
            }
            ArrayList<String> urls = database.getChannelDocUrls(channel);

            StringBuilder header = new StringBuilder();
            header.
                    append("<html>\n").append("<body>\n").
                    append("<div class=”channelheader”> Channel name: ").append(channel).
                    append(" created by: ").append(author).append(":</div>\n");

            StringBuilder body = new StringBuilder("<body>\n");
            for (String url: urls) {
                String date = database.getDocumentTime(url);
                String doc = database.getDocument(url);
                body.
                        append("Crawled on: ").append(date).
                        append(" Location: ").append(url).append("\n").
                        append("<div class=”document”>").append(doc).append("</div>").append("\n");
            }
            body.append("</body>\n").append("</html>");

            return header.toString() + body.toString();
        });
        get("/subscribe", (req, res) -> {
            String channel = req.queryParams("channel");
            String user = req.session(false).attribute("user");
            boolean success = database.addSubscription(user, channel);
            if (success) {
                res.status(200);
                return "Subscribe to channel: " + channel + " success";
            } else {
                halt(400, "Bad Request");
                return "Subscription failed";
            }
        });
        awaitInitialization();
    }
}
