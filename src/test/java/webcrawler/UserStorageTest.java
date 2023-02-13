package webcrawler;

import webcrawler.storage.StorageFactory;
import webcrawler.storage.StorageInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class UserStorageTest {
    StorageInterface db;
    String dir = "./testdb";

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
        db = StorageFactory.getDatabaseInstance(dir);
    }
    @Test
    public void testGetUserSuccess() {
        String user = "ss";
        String pass = "sss";
        db.addUser(user, pass);
        assert (db.getSessionForUser(user, pass));
    }
    @Test
    public void testGetUserFail() {
        String user = "sss";
        String pass = "sss";
        db.addUser(user, pass);
        assert (!db.getSessionForUser(user, "123"));
    }
    @After
    public void tearDown() {
        db.close();
        deleteDir(new File(dir));
    }
}
