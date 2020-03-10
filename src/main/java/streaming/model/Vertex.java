package streaming.model;

import java.util.*;

public class Vertex {
    private Map<String, String> properties;
    private Set<Long> memberships;
    private String label;

    public Vertex(org.s1ck.gdl.model.Vertex gdlVertex){
        this();
        for(Map.Entry<String, Object> prop : gdlVertex.getProperties().entrySet()){
            properties.put(prop.getKey(),prop.getValue().toString());
        }
        memberships = gdlVertex.getGraphs();
        label = gdlVertex.getLabel();
    }

    public Vertex() {
        this.properties = new HashMap<>();
        this.memberships = new HashSet<>();
        label = "";
    }

    public String getProperty(String key) {
        return properties.get(key);
    }

    public void put(String key, String value) {
        properties.put(key, value);
    }

    public Set<Long> getMemberships() {
        return memberships;
    }

    public void addMembership(Long membership) {
        this.memberships.add(membership);
    }

    public void removeMembership(String membership) {
        this.memberships.remove(membership);
    }

    @Override
    public String toString() {
        return String.format("(%s properties=%s, memberships=%s)",
                label, properties, memberships);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vertex vertex = (Vertex) o;
        return Objects.equals(properties, vertex.properties) &&
                Objects.equals(memberships, vertex.memberships) &&
                Objects.equals(label, vertex.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties, memberships, label);
    }
}
