package webcrawler.storage;

import com.sleepycat.collections.StoredSortedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class StorageManager implements StorageInterface {
    static final Logger logger = LogManager.getLogger(StorageManager.class);
    private final Storage st;
    private final StoredSortedMap<String, UserData> userMap;
    private final StoredSortedMap<String, DocData> docMap;
    private final StoredSortedMap<String, String> seenMap;
    private final StoredSortedMap<String, ChannelData> channelMap;

    public StorageManager(Storage st) {
        this.st = st;
        this.userMap = st.getViews().getUserMap();
        this.docMap = st.getViews().getDocMap();
        this.seenMap = st.getViews().getSeenMap();
        this.channelMap = st.getViews().getChannelMapMap();
    }
    /**
     * How many documents so far?
     */
    @Override
    public int getCorpusSize() {
        return docMap.size();
    }

    /**
     * Add a new document, getting its ID
     *
     * @param url
     * @param documentContents
     * @param type
     */
    @Override
    public void addDocument(String url, String documentContents, String type) {
        String md5 = encode(documentContents, "MD5");
        if (!alreadyExist(md5)) {
            docMap.put(url, new DocData(documentContents, new Date().getTime(), type));
            seenMap.put(md5, url);
            st.getDocDatabase().sync();
            st.getCatalogDatabase().sync();
            logger.info("Add document: [url: " + url + " type: " + type + "]");
        } else {
            logger.info("Document Already Exists");
        }

    }

    /**
     * Retrieves a document's contents by URL
     *
     * @param url
     */
    @Override
    public String getDocument(String url) {
        DocData doc = docMap.getOrDefault(url, null);
        if (doc != null) {
            logger.info("Get document: [url: " + url + "]");
            return doc.getContent();
        } else {
            return null;
        }
    }

    @Override
    public String getDocumentTime(String url) {
        DocData doc = docMap.getOrDefault(url, null);
        if (doc != null) {
            SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
            return f.format(new Date(doc.getLastModified()));
        } else {
            return null;
        }
    }

    /**
     * Retrieves a document's type by URL
     *
     * @param url
     */
    @Override
    public String getType(String url) {
        DocData doc = docMap.getOrDefault(url, null);
        if (doc != null) {
            String type = doc.getType();
            logger.info("Type: " + type);
            return type;
        }
        return null;
    }

    /**
     * Adds a user and returns an ID
     *
     * @param username
     * @param password
     */
    @Override
    public void addUser(String username, String password) {
        String encoded = encode(password, "SHA-256");
        userMap.put(username, new UserData(username, encoded));
        st.getUserDatabase().sync();
        st.getCatalogDatabase().sync();
        logger.info("Add user: [username: " + username + "]");
    }

    /**
     * Tries to log in the user, or else throws a HaltException
     *
     * @param username
     * @param password
     */
    @Override
    public boolean getSessionForUser(String username, String password) {
        String encoded = encode(password, "SHA-256");
        UserData user = userMap.getOrDefault(username, null);
        if (user != null) {
            if (user.getPassword().equals(encoded)) {
                logger.info("Get Session For User: "  + username);
                return true;
            } else {
                logger.info("Wrong password");
            }
        } else {
            logger.info("User not exist");
        }
        return false;
    }

    /**
     * Update a new channel (On create)
     *
     * @param name
     * @param Xpath
     * @param author
     */
    @Override
    public boolean updateChannel(String name, String Xpath, String author, String docUrl) {
        if (Xpath == null || author == null) {
            return updateChannel(name, docUrl);
        } else {
            if (!channelMap.containsKey(name)) {
                ChannelData channel = new ChannelData(name, author, Xpath);
                if (docUrl != null) channel.addDocUrl(docUrl);
                channelMap.put(name, channel);
                st.getChannelDatabase().sync();
                st.getCatalogDatabase().sync();
                logger.info("Create Channel: " + name + " by: " + author);
                return true;
            } else {
                logger.info("Channel Existed, Cannot Create: " + name);
                return false;
            }
        }
    }

    public boolean updateChannel(String name, String docUrl) {
        String author = getChannelAuthor(name);
        ArrayList<String> urls = getChannelDocUrls(name);
        String Xpath = getChannelXpath(name);
        if (author == null || urls == null || Xpath == null) {
            logger.info("Nonsense");
            return false;
        }
        ChannelData channel = new ChannelData(name, author, Xpath);
        urls.add(docUrl);
        channel.replaceDocUrls(urls);
        channelMap.put(name, channel);
        st.getChannelDatabase().sync();
        st.getCatalogDatabase().sync();
        logger.info("Update Channel: " + name + " with: " + docUrl);
        return true;
    }

    /**
     * Retrieves a channel contents by URL
     *
     * @param name
     */
    @Override
    public ArrayList<String> getChannelDocUrls(String name) {
        if (channelMap.containsKey(name)) {
            return channelMap.get(name).getDocUrls();
        }
        return null;
    }

    @Override
    public String getChannelAuthor(String name) {
        if (channelMap.containsKey(name)) {
            return channelMap.get(name).getAuthor();
        }
        return null;
    }

    /**
     * Retrieves all channel Xpaths
     */
    @Override
    public HashMap<String, String[]> getChannelsXpaths() {
        HashMap<String, String[]> pair = new HashMap<>();
        ArrayList<String> Channels = new ArrayList<>();
        ArrayList<String> Xpaths = new ArrayList<>();
        channelMap.forEach((key, val) -> {
            Channels.add(key);
            Xpaths.add(val.getXpath());
        });
        String[] channels = Channels.toArray(new String[0]);
        String[] xpaths = Xpaths.toArray(new String[0]);
        pair.put("channels", channels);
        pair.put("Xpaths", xpaths);
        return pair;
    }

    public String getChannelXpath(String name) {
        if (channelMap.containsKey(name)) {
            return channelMap.get(name).getXpath();
        }
        return null;
    }

    @Override
    public HashMap<String, ArrayList<String>> getSubscriptionsByUser(String name) {
        HashMap<String, ArrayList<String>> maps = new HashMap<>();
        if (userMap.containsKey(name)) {
            ArrayList<String> channels = userMap.get(name).getSubscriptions();
            for (String channel: channels) {
                if (channelMap.containsKey(channel)) {
                    maps.put(channel, channelMap.get(channel).getDocUrls());
                }
            }
        }
        return maps;
    }

    @Override
    public boolean addSubscription(String username, String channel) {
        if (userMap.containsKey(username)) {
            UserData user = userMap.get(username);
            if (user.getSubscriptions().contains(channel))
                return false;
            if (channelMap.containsKey(channel)) {
                user.addSubscription(channel);
                userMap.put(username, user);
                st.getUserDatabase().sync();
                st.getCatalogDatabase().sync();
                logger.info("User: " + username + " Subscibe to Channel: " + channel);
                return true;
            }
        }
        return false;
    }

    /**
     * Shuts down / flushes / closes the storage system
     */
    @Override
    public void close() {
        st.close();
    }

    public boolean userExist(String username) {
        return userMap.containsKey(username);
    }

    public static String encode(String base, String algorithm) {
        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] hash = digest.digest(base.getBytes());
            StringBuilder hexString = new StringBuilder();

            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public boolean alreadyExist(String md5) {
        return st.alreadyExist(md5);
    }

    public boolean isModified(String url, long time) {
        DocData doc = docMap.getOrDefault(url, null);
        if (doc != null) {
            return doc.getLastModified() < time;
        }
        return false;
    }
}
