package streaming.model;

import java.util.*;

public class Vertex {
    private String variable;
    private String label;
    private Map<String, String> properties;
    private Set<Long> memberships;

    public Vertex() {
        this.properties = new HashMap<>();
        this.memberships = new HashSet<>();
        label = "";
        variable = Integer.toString(this.hashCode());
    }

    public Vertex(org.s1ck.gdl.model.Vertex gdlVertex) {
        this();
        for (Map.Entry<String, Object> prop : gdlVertex.getProperties().entrySet()) {
            properties.put(prop.getKey(), prop.getValue().toString());
        }
        memberships = gdlVertex.getGraphs();
        label = gdlVertex.getLabel();
        variable = gdlVertex.getVariable();
    }

    public String getVariable() {
        return variable;
    }

    public void setVariable(String variable) {
        this.variable = variable;
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

    public String addProperty(String key, String value) {
        return properties.put(key, value);
    }

    public String removeProperty(String key) {
        return properties.remove(key);
    }

    public Set<Long> getMemberships() {
        return memberships;
    }

    public boolean addMembership(Long membership) {
        return this.memberships.add(membership);
    }

    public boolean removeMembership(String membership) {
        return this.memberships.remove(membership);
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
