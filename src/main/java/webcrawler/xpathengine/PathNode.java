package webcrawler.xpathengine;

public class PathNode {
    public int queryId1;
    public int queryId2;
    public int level;
    public int length;
    public String nodeName;
    public String filter = null;

    public PathNode(int queryId1, int queryId2, int length, String nodeName) {
        this.queryId1 = queryId1;
        this.queryId2 = queryId2;
        this.level = queryId2;
        this.nodeName = nodeName;
        this.length = length;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String toString() {
        return "PathNode: " + queryId1 + "-" + queryId2 + " Name " + nodeName + " Level " + level + " Filter " + filter;
    }

    @Override
    public boolean equals(Object obj) {
        assert obj.getClass().equals(this.getClass());
        return this.queryId1 == ((PathNode) obj).queryId1 && this.queryId2 == ((PathNode) obj).queryId2;
    }
}
