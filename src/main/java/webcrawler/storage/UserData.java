package webcrawler.storage;

import java.io.Serializable;
import java.util.ArrayList;

public class UserData implements Serializable {
    public static final long serialVersionUID = -3040096452457271695L;
    private final String username;
    private final String password;
    private final ArrayList<String> subscriptions;

    public UserData(String username, String password) {
        this.username = username;
        this.password = password;
        this.subscriptions = new ArrayList<>();
    }

    public final String getUserName()
    {
        return username;
    }

    public final String getPassword()
    {
        return password;
    }

    public final ArrayList<String> getSubscriptions() {
        return subscriptions;
    }

    public final void addSubscription(String channel) {
        subscriptions.add(channel);
    }

    public String toString()
    {
        return "[UserData: username=" + username + " password=" + password + ']';
    }
}
