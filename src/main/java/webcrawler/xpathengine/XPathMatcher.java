package webcrawler.xpathengine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class XPathMatcher {
    static Logger logger = LogManager.getLogger(XPathMatcher.class);

    private final HashMap<String, HashMap<String, ArrayList<PathNode>>> QueryIndex;
    private final ArrayList<ArrayList<PathNode>> pathNodesList;
    private final ArrayList<PathNode> changeStack;
    private final boolean[] bitmap;
    private final boolean[] textmap;

    public XPathMatcher(ArrayList<ArrayList<PathNode>> pathNodesList) {
        this.pathNodesList = pathNodesList;
        this.QueryIndex = new HashMap<>();
        this.changeStack = new ArrayList<>();
        this.bitmap = new boolean[pathNodesList.size()];
        this.textmap = new boolean[pathNodesList.size()];
        Arrays.fill(bitmap, false);
        Arrays.fill(textmap, false);
        this.buildQueryIndex();
    }

    public boolean[] getBitmap() {
        return bitmap;
    }

    public HashMap<String, HashMap<String, ArrayList<PathNode>>> getQueryIndex() {
        return this.QueryIndex;
    }

    private void buildQueryIndex() {
        for (ArrayList<PathNode> nodes: pathNodesList) {
            for (int i = 0; i < nodes.size(); i++) {
                PathNode node = nodes.get(i);
                HashMap<String, ArrayList<PathNode>> twoLists;
                ArrayList<PathNode> CL;
                ArrayList<PathNode> WL;
                if (!QueryIndex.containsKey(node.nodeName)) {
                    twoLists = new HashMap<>();
                    CL = new ArrayList<>();
                    WL = new ArrayList<>();
                } else {
                    twoLists = QueryIndex.get(node.nodeName);
                    CL = twoLists.get("CL");
                    WL = twoLists.get("WL");
                }
                if (i == 0) {
                    CL.add(node);
                } else {
                    WL.add(node);
                }
                twoLists.put("CL", CL);
                twoLists.put("WL", WL);
                QueryIndex.put(node.nodeName, twoLists);
            }
        }
    }

    private HashMap<String, ArrayList<PathNode>> getHTMLEntry(String value) {
        for (Map.Entry<String, HashMap<String, ArrayList<PathNode>>> entry : QueryIndex.entrySet()) {
            String key = entry.getKey();
            if (key.equalsIgnoreCase(value)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private void updateQueryIndex(String nodeName) {
        boolean isContain = false;
        for (PathNode node : changeStack) {
            if (node.nodeName.equals(nodeName)) {
                isContain = true;
                break;
            }
        }
        if (isContain) return;

        HashMap<String, ArrayList<PathNode>> nextList = QueryIndex.getOrDefault(nodeName, null);

        if (nextList != null) {
//            logger.debug("---------updateQueryIndex(" + nodeName + ")---------");
            ArrayList<PathNode> nextCL = nextList.get("CL");
            ArrayList<PathNode> nextWL = nextList.get("WL");
//            for (PathNode node: nextCL){
//                logger.debug("UPDATE BEFORE: " + nodeName + " " + node.toString());
//            }
            int wl = nextWL.size();
            int cl = nextCL.size();
            if (wl != 0) {
                if (cl != 0) {
                    for (int k = 0; k < wl; k++) {
                        for (int l = 0; l < cl; l++) {
                            if (!(nextWL.get(k).queryId1 == nextCL.get(l).queryId1
                                    && nextWL.get(k).queryId2 == nextCL.get(l).queryId2)) {
                                nextCL.add(nextWL.get(k));
                                changeStack.add(nextWL.get(k));
                            }
                        }
                    }
                } else {
                    nextCL.addAll(nextWL);
                    changeStack.addAll(nextWL);
                }
//                for (PathNode node: nextCL){
//                    logger.debug("UPDATE AFTER: " + nodeName + " " + node.toString());
//                }
            }
            nextList.put("CL", nextCL);
            QueryIndex.put(nodeName, nextList);
//            logger.debug("UPDATE CHANGE STACK: " + changeStack.toString());
        }
    }

    private void recoverQueryIndex(String name) {
        boolean isContain = false;
        PathNode nextNode = null;
        for (PathNode node : changeStack) {
            if (node.nodeName.equalsIgnoreCase(name)) {
                isContain = true;
                nextNode = node;
                break;
            }
        }
        if (!isContain) return;
        // BACKTRACK
        int size = changeStack.size() - 1;
        for (int i = size; i >= 0 ; i--) {
            if (changeStack.get(i).nodeName.equalsIgnoreCase(nextNode.nodeName)) break;
            PathNode curNode = changeStack.get(i);
            if (curNode.level == nextNode.level) continue;
            HashMap<String, ArrayList<PathNode>> twoList = QueryIndex.getOrDefault(curNode.nodeName, null);

            if (twoList != null) {
//                logger.debug("---------RECOVER ENTRY: " + cur + "---------");
                ArrayList<PathNode> CL = twoList.get("CL");
                ArrayList<PathNode> WL = twoList.get("WL");
//                for (PathNode node: CL){
//                    logger.debug("RECOVERY BEFORE: " + node.toString());
//                }
                for (PathNode node: WL){
                    CL.remove(node);
                }
//                for (PathNode node: CL){
//                    logger.debug("RECOVERY AFTER: " + node.toString());
//                }
                twoList.put("CL", CL);
                QueryIndex.put(curNode.nodeName, twoList);
                changeStack.remove(i);
//                logger.debug("RECOVERY CHANGE STACK: " + changeStack.toString());
            }
        }
    }

    public void handleOpen(OccurrenceEvent event) {
        String value = event.getValue();
        int level = event.getLevel();
        boolean isHTML = !event.url.endsWith(".xml");

        HashMap<String, ArrayList<PathNode>> twoList;
        if (isHTML) {
            twoList = getHTMLEntry(value);
        } else {
            twoList = QueryIndex.getOrDefault(value, null);
        }

        if (twoList != null) {
            ArrayList<PathNode> CL = twoList.get("CL");
            for (PathNode node: CL) {
                // Level Check
//                logger.debug("QUERY: --- " + node.nodeName + " --- " + node.level + " --- " + node.length + " --- " + node.queryId1 + "-" + node.queryId2);
//                logger.debug("EVENT: --- " + value + " --- " + event.level + " --- " + event.url);
                if (node.level == level) {
//                    logger.debug("HIT! " + node.toString());
                    int i = node.queryId1 - 1;
                    int j = node.queryId2;
                    if (j != node.length) {
                        updateQueryIndex(pathNodesList.get(i).get(j).nodeName);
                    } else {
                        bitmap[i] = true;
                    }
//                    logger.debug(this.toString());
                }
            }
        }
    }

    public void handleClose(OccurrenceEvent event) {
        String name = event.getValue();
        recoverQueryIndex(name);
    }

    public void handleText(OccurrenceEvent event) {
        String text = event.getValue().trim();
        String name = event.getParent();
        int level = event.getLevel();
        boolean isHTML = !(event.url.endsWith(".xml"));

        HashMap<String, ArrayList<PathNode>> twoList;

        if (isHTML) {
            twoList = getHTMLEntry(name);
        } else {
            twoList = QueryIndex.getOrDefault(name, null);
        }

        if (twoList != null) {
//            logger.debug("TAG: " + tag + " TEXT: " + text);
            ArrayList<PathNode> CL = twoList.get("CL");
            for (PathNode node: CL) {
//                logger.debug(node.toString());
                if (node.filter == null) continue;
                // Level Check
                if (node.level == level) {
                    // Filter Check
                    String filter = node.filter;
                    boolean checked = true;

                    int p = filter.indexOf("\"");
                    assert p != -1;
                    filter = filter.substring(p+1);
                    p = filter.indexOf("\"");
                    filter = filter.substring(0,p);

                    if (node.filter.contains("contains")) {
                        if (!text.contains(filter)) checked = false;
                    } else {
                        if (!text.equals(filter)) checked = false;
                    }

                    int i = node.queryId1 - 1;
                    int j = node.queryId2;

                    if (textmap[i]) return;

                    if (j != node.length) {
                        String nodeName = pathNodesList.get(i).get(j).nodeName;
                        if (checked) {
                            updateQueryIndex(nodeName);
                        } else {
                            recoverQueryIndex(nodeName);
                        }
                    } else {
//                        logger.debug("-------- " + event.url + " ---------");
//                        logger.debug("--------CHECK: " + checked + "---------");
//                        logger.debug("--------TEXT: " + text + "---------");
//                        logger.debug("--------FILTER: " + filter + "---------");
                        if (checked && !textmap[i]) {
                            textmap[i] = true;
                            bitmap[i] = true;
                        }
                        bitmap[i] = checked;
                    }
                }
            }
        }
    }

    public String toString() {
        StringBuilder str = new StringBuilder();
        for (Map.Entry<String, HashMap<String, ArrayList<PathNode>>> entry : QueryIndex.entrySet()) {
            String key = entry.getKey();
            HashMap<String, ArrayList<PathNode>> twoLists = entry.getValue();
            str.append("\n").append(key).append(":\n");
            str.append("  CL: \n");
            for (PathNode node: twoLists.get("CL"))
                str.append("    ").append(node.toString()).append("\n");
            str.append("  WL: \n");
            for (PathNode node: twoLists.get("WL"))
                str.append("    ").append(node.toString()).append("\n");
        }
        return str.toString();
    }
}
