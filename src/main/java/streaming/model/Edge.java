package streaming.model;

import java.util.*;

public class Edge {

    private Vertex source;
    private Vertex target;
    private Map<String, String> properties;
    private Set<Long> memberships;
    private String label;

    public Edge() {
        this.source = new Vertex();
        this.target = new Vertex();
        this.properties = new HashMap<>();
        this.memberships = new HashSet<>();
        label = "";
    }


    public Edge(Vertex source, Vertex target, String label, Map<String, String> properties, Set<Long> memberships) {
        this.source = source;
        this.target = target;
        this.label = label;
        this.properties = properties;
        this.memberships = memberships;
    }

    public Edge(String fromStr, String toStr, String contentStr) {
        this();
        source.put("id", fromStr);
        target.put("id", toStr);
        properties.put("id", contentStr);
        memberships.add(0L);
    }

    public Edge(org.s1ck.gdl.model.Edge gdlEdge, org.s1ck.gdl.model.Vertex gdlSourceV, org.s1ck.gdl.model.Vertex gdlTargetV) {
        this.source = new Vertex(gdlSourceV);
        this.target = new Vertex(gdlTargetV);
        properties = new HashMap<>();
        for (Map.Entry<String, Object> prop : gdlEdge.getProperties().entrySet()) {
            properties.put(prop.getKey(), prop.getValue().toString());
        }
        memberships = gdlEdge.getGraphs();
        label = gdlEdge.getLabel();
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }


    public String getProperty(String key) {
        return properties.get(key);
    }

    public void addProperty(String key, String value) {
        properties.put(key, value);
    }

    public Vertex getSource() {
        return source;
    }

    public void setSource(Vertex newFrom) {
        source = newFrom;
    }

    public Vertex getTarget() {
        return target;
    }

    public Map<String, String> getProperties() {
        return properties;
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
        return String.format("%s--%s-->%s",
                source, getEdgeInfoString(), target);
    }

    private String getEdgeInfoString() {
        return String.format("[%s properties=%s memberships=%s]",
                label, properties, memberships);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return Objects.equals(source, edge.source) &&
                Objects.equals(target, edge.target) &&
                Objects.equals(properties, edge.properties) &&
                Objects.equals(memberships, edge.memberships) &&
                Objects.equals(label, edge.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target, properties, memberships, label);
    }
}
