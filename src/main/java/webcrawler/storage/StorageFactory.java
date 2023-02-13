package webcrawler.storage;

public class StorageFactory {
    private static StorageInterface db;
    public static StorageInterface getDatabaseInstance(String directory) {
        if (db == null)
            db = new Storage(directory).getStorageManager();
        return db;
    }
}
