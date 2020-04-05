package streaming.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Edge {

    private Vertex source;
    private Vertex target;

    private GraphElementInformation gei;

    private boolean reverse;

    public Edge() {
        this.source = new Vertex();
        this.target = new Vertex();
        this.gei = new GraphElementInformation();
    }

    public Edge(Edge otherEdge) {
        source = otherEdge.getSource();
        target = otherEdge.getTarget();
        gei = otherEdge.getGei();
    }


    public Edge(Vertex source, Vertex target, GraphElementInformation gei) {
        this.source = source;
        this.target = target;
        this.gei = gei;
    }

    public Edge(String fromStr, String toStr, String contentStr) {
        this.source = new Vertex(fromStr);
        this.source = new Vertex(toStr);
        this.gei = new GraphElementInformation();
        gei.addProperty("id", contentStr);
        gei.addMembership(0L);
    }

    public Edge(org.s1ck.gdl.model.Edge gdlEdge, org.s1ck.gdl.model.Vertex gdlSourceV, org.s1ck.gdl.model.Vertex gdlTargetV) {
        this.source = new Vertex(gdlSourceV);
        this.target = new Vertex(gdlTargetV);
        HashMap<String, String> properties = new HashMap<>();
        for (Map.Entry<String, Object> prop : gdlEdge.getProperties().entrySet()) {
            properties.put(prop.getKey(), prop.getValue().toString());
        }
        Set<Long> memberships = gdlEdge.getGraphs();
        String label = gdlEdge.getLabel();
        this.gei = new GraphElementInformation(label, properties, memberships);
    }


    public Vertex getSource() {
        return source;
    }

    public void setSource(Vertex newSource) {
        source = newSource;
    }

    public Vertex getTarget() {
        return target;
    }

    public void setTarget(Vertex newTarget) {
        target = newTarget;
    }

    public GraphElementInformation getGei() {
        return gei;
    }

    public void setGei(GraphElementInformation gei) {
        this.gei = gei;
    }

    public boolean isReverse() {
        return reverse;
    }

    public Edge createReverseEdge() {
        Edge reverseEdge = new Edge(this.getTarget(), this.getSource(), this.getGei());
        reverseEdge.reverse = true;
        return reverseEdge;
    }

    @Override
    public String toString() {
        return String.format("%s--[%s reverse=%b]-->%s",
                source, gei, reverse, target);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return Objects.equals(source, edge.source) &&
                Objects.equals(target, edge.target) &&
                Objects.equals(gei, edge.gei);
    }

    @Override
    public int hashCode() {
        return Objects.hash(source, target, gei);
    }
}
