package webcrawler;

import webcrawler.storage.StorageFactory;
import webcrawler.storage.StorageInterface;
import webcrawler.storage.StorageManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;

public class DocStorageTest {

    StorageManager db;
    String dir = "./testdb";
    String url = "https://test.com/";
    String content = "test";
    String type = "text/html";

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
    }
    @Test
    public void testGetDocSuccess() {
        assert (db.getDocument(url).contains(content));
    }
    @Test
    public void testAlreadyExist() {
        String md5 = StorageManager.encode(content, "MD5");
        assert (db.alreadyExist(md5));
    }
    @Test
    public void testIsModified() {
        long time = new Date().getTime() + 2000;
        assert (db.isModified(url, time));
    }
    @After
    public void tearDown() {
        db.close();
        deleteDir(new File(dir));
    }
}
