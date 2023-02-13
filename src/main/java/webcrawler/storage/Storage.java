package webcrawler.storage;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.*;

import java.io.File;

public class Storage {
    private final String DOCUMENT_STORE = "document_store";
    private final String USER_STORE = "user_store";
    private final String CONTENT_SEEN = "content_seen";
    private final String CHANNEL_STORE = "channel_store";
    private final String CLASS_CATALOG = "java_class_catalog";

    private final Environment env;
    private final Database docDb;
    private final Database userDb;
    private final Database seenDb;
    private final Database channelDb;
    private final Database catalogDb;
    private final StoredClassCatalog javaCatalog;

    private final Views views;

    public Storage(String directory) {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        env = new Environment(new File(directory), envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setDeferredWrite(true);
        catalogDb = env.openDatabase(null, CLASS_CATALOG, dbConfig);
        javaCatalog = new StoredClassCatalog(catalogDb);
        docDb = env.openDatabase(null, DOCUMENT_STORE, dbConfig);
        userDb = env.openDatabase(null, USER_STORE, dbConfig);
        channelDb = env.openDatabase(null, CHANNEL_STORE, dbConfig);
        // SeenDb is temporary
        DatabaseConfig seenConfig = new DatabaseConfig();
        seenConfig.setAllowCreate(true);
        seenConfig.setTemporary(true);
        seenDb = env.openDatabase(null, CONTENT_SEEN, seenConfig);
        // Create Schemas
        views = new Views(javaCatalog, docDb, userDb, seenDb, channelDb);
    }

    public StorageInterface getStorageManager() {
        return new StorageManager(this);
    }

    public Database getUserDatabase()
    {
        return userDb;
    }

    public Database getDocDatabase()
    {
        return docDb;
    }

    public Database getSeenDatabase() {
        return seenDb;
    }

    public Database getChannelDatabase() {
        return channelDb;
    }

    public Database getCatalogDatabase() {
        return catalogDb;
    }

    public Views getViews() {
        return views;
    }

    public void close() throws DatabaseException
    {
        userDb.close();
        docDb.close();
        seenDb.close();
        channelDb.close();
        javaCatalog.close();
        env.close();
    }

    public boolean alreadyExist(String md5) {
        if(views.getSeenMap().containsKey(md5)) {
            return true;
        }
        return false;
    }
}
