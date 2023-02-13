package webcrawler.storage;

import java.util.ArrayList;
import java.util.HashMap;

public interface StorageInterface {

    /**
     * How many documents so far?
     */
    public int getCorpusSize();

    /**
     * Add a new document
     */
    public void addDocument(String url, String documentContents, String type);

    /**
     * Retrieves a document's contents by URL
     */
    public String getDocument(String url);
    public String getDocumentTime(String url);

    /**
     * Retrieves a document's type by URL
     */
    public String getType(String url);

    /**
     * Adds a user
     */
    public void addUser(String username, String password);

    /**
     * Tries to log in the user, or else throws a HaltException
     */
    public boolean getSessionForUser(String username, String password);

    /**
     * Update a new channel (On create)
     */
    public boolean updateChannel(String name, String Xpath, String author, String docUrl);

    /**
     * Retrieves a channel contents by URL
     */
    public ArrayList<String> getChannelDocUrls(String name);
    public String getChannelAuthor(String name);

    /**
     * Retrieves all channel Xpaths
     */
    public HashMap<String, String[]> getChannelsXpaths();

    public HashMap<String, ArrayList<String>> getSubscriptionsByUser(String name);
    public boolean addSubscription(String user, String channel);

    /**
     * Shuts down / flushes / closes the storage system
     */
    public void close();

}
