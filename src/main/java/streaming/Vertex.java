package streaming;

import java.util.HashMap;

public class Vertex {
    private HashMap<String, String> content;

    public Vertex() {
        this.content = new HashMap<>();
    }

    public String get(String key) {
        return content.get(key);
    }

    public void put(String key, String value) {
        content.put(key, value);
    }

    @Override
    public String toString() {
        return "<Vertex " +
                 content +
                '>';
    }
}
