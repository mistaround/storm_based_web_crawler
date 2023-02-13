package webcrawler;

import webcrawler.storage.StorageFactory;
import webcrawler.storage.StorageManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
public class UserChannelTest {
    StorageManager db;
    String dir = "./testdb";
    String url = "https://test.com/";
    String content = "This is a test file";
    String type = "text/html";
    String channel = "test";
    String Xpath = "/a/b/c";
    String author = "testUser";

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
    @Before
    public void setUp() {
        if (!Files.exists(Paths.get(dir))) {
            try {
                Files.createDirectory(Paths.get(dir));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        db = (StorageManager) StorageFactory.getDatabaseInstance(dir);
        db.addDocument(url, content, type);
        db.addUser(author, author);
    }
    @Test
    public void testCreateUpdateSubscribeGetChannelXPaths() {
        db.updateChannel(channel, Xpath, author, null);
        assert (db.getChannelAuthor(channel).equals(author));

        db.updateChannel(channel, url);
        assert (db.getChannelDocUrls(channel).contains(url));

        assert (db.getChannelsXpaths().get("channels")[0].equals(channel));
        assert (db.getChannelsXpaths().get("Xpaths")[0].equals(Xpath));

        db.addSubscription(author, channel);
        assert (db.getSubscriptionsByUser(author).get(channel).contains(url));
    }

    @After
    public void tearDown() {
        db.close();
        deleteDir(new File(dir));
    }
}
