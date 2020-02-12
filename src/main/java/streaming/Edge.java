package streaming;

import java.util.HashMap;

public class Edge {

    private Vertex from;
    private Vertex to;
    private HashMap<String, String> content;

    public Edge() {
        this.from = new Vertex();
        this.to = new Vertex();
        this.content = new HashMap<>();
    }


    public Edge(Vertex from, Vertex to, HashMap<String,String> content) {
        this.from = from;
        this.to = to;
        this.content = content;
    }

    public Edge(String fromStr, String toStr, String contentStr){
        this();
        from.put("id", fromStr);
        to.put("id", toStr);
        content.put("id", contentStr);
    }


    public String get(String key) {
        return content.get(key);
    }

    public void put(String key, String value) {
        content.put(key, value);
    }


    public Vertex getFrom() {
        return from;
    }

    public Vertex getTo() {
        return to;
    }

    public HashMap<String, String> getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "("+ from + ")--[" + content + "]-->("+ to+ ")";
    }
}
