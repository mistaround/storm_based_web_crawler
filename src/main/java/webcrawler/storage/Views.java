package webcrawler.storage;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredEntrySet;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;

public class Views {
    private final StoredSortedMap<String, UserData> userMap;
    private final StoredSortedMap<String, DocData> docMap;
    private final StoredSortedMap<String, String> seenMap;
    private final StoredSortedMap<String, ChannelData> channelMap;

    public Views(StoredClassCatalog catalog, Database docDb, Database userDb, Database seenDb, Database channelDb) {
        EntryBinding<String> userKeyBinding = new SerialBinding<>(catalog, String.class);
        EntryBinding<UserData> userDataBinding = new SerialBinding<>(catalog, UserData.class);

        EntryBinding<String> docKeyBinding = new SerialBinding<>(catalog, String.class);
        EntryBinding<DocData> docDataBinding = new SerialBinding<>(catalog, DocData.class);

        EntryBinding<String> seenKeyBinding = new SerialBinding<>(catalog, String.class);
        EntryBinding<String> seenDataBinding = new SerialBinding<>(catalog, String.class);

        EntryBinding<String> channelKeyBinding = new SerialBinding<>(catalog, String.class);
        EntryBinding<ChannelData> channelDataBinding = new SerialBinding<>(catalog, ChannelData.class);

        userMap = new StoredSortedMap<>(userDb, userKeyBinding, userDataBinding, true);
        docMap = new StoredSortedMap<>(docDb, docKeyBinding, docDataBinding, true);
        seenMap = new StoredSortedMap<>(seenDb, seenKeyBinding, seenDataBinding, true);
        channelMap = new StoredSortedMap<>(channelDb, channelKeyBinding, channelDataBinding, true);
    }

    public final StoredSortedMap<String, UserData> getUserMap()
    {
        return userMap;
    }

    public final StoredSortedMap<String, DocData>  getDocMap()
    {
        return docMap;
    }

    public final StoredSortedMap<String, String>  getSeenMap()
    {
        return seenMap;
    }

    public final StoredSortedMap<String, ChannelData>  getChannelMapMap()
    {
        return channelMap;
    }


    public final StoredEntrySet<String, UserData> getUserEntrySet()
    {
        return (StoredEntrySet<String, UserData>) userMap.entrySet();
    }

    public final StoredEntrySet<String, DocData> getDocEntrySet()
    {
        return (StoredEntrySet<String, DocData>) docMap.entrySet();
    }

    public final StoredEntrySet<String, String> getSeenEntrySet()
    {
        return (StoredEntrySet<String, String> ) seenMap.entrySet();
    }

    public final StoredEntrySet<String, ChannelData> getChannelEntrySet()
    {
        return (StoredEntrySet<String, ChannelData>) channelMap.entrySet();
    }

}
