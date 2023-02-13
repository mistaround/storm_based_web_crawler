package webcrawler.crawler.handlers;

import webcrawler.storage.StorageInterface;
import spark.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LogoutHandler implements Route {
    static final Logger logger = LogManager.getLogger(LogoutHandler.class);
    StorageInterface db;

    public LogoutHandler(StorageInterface db) {
        this.db = db;
    }

    @Override
    public String handle(Request req, Response resp) throws HaltException {

        logger.info("Request to Logout");
        Session session = req.session(false);
        if (session != null) {
            logger.info("Logged out");
            session.invalidate();
        } else {
            logger.info("Did not login");
        }
        resp.redirect("/login-form.html");

        return "";
    }
}
