package webcrawler.crawler.handlers;

import webcrawler.storage.StorageInterface;
import webcrawler.storage.StorageManager;
import spark.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegistrationHandler implements Route {
    static final Logger logger = LogManager.getLogger(RegistrationHandler.class);
    StorageManager db;

    public RegistrationHandler(StorageInterface db) {
        this.db = (StorageManager)db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {
        String user = req.queryParams("username");
        String pass = req.queryParams("password");

        logger.info("Registration request for " + user + " and " + pass);
        if (db.userExist(user)) {
            logger.info("Username used!");
            resp.status(419);
            resp.redirect("/conflict.html");
        } else {
            logger.info("Registration Success!");
            db.addUser(user, pass);
            resp.status(200);
            return "<!DOCTYPE html>\n" +
                    "<html>\n" +
                    "<head>\n" +
                    "    <title>Registration Success</title>\n" +
                    "</head>\n" +
                    "<body>\n" +
                    "<h1><a href=\"login-form.html\">login<a> </h1>\n" +
                    "</body>\n" +
                    "</html>";
        }

        return "";
    }
}
