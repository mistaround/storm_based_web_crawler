package webcrawler.xpathengine;

/**
 This class encapsulates the tokens we care about parsing in XML (or HTML)
 */
public class OccurrenceEvent {
	public enum Type {Open, Close, Text};
	
	Type type;
	String value;
	String url;
	String parent = null;
	int level;
	
	public OccurrenceEvent(Type t, String value, String url, int level) {
		this.type = t;
		this.value = value;
		this.url = url;
		this.level = level;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getUrl() {
		return url;
	}

	public int getLevel() {
		return level;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}
	

	public String toString() {
		if (type == Type.Open) 
			return "<" + value + ">";
		else if (type == Type.Close)
			return "</" + value + ">";
		else
			return value;
	}
}
