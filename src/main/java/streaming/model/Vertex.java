package streaming.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Vertex {

  private GraphElementInformation gei;

  public Vertex() {
    this.gei = new GraphElementInformation();
  }

  public Vertex(String contentStr) {
    this.gei = new GraphElementInformation();
    gei.addProperty("id", contentStr);
  }

  public Vertex(GraphElementInformation gei) {
    this.gei = gei;
  }

  public Vertex(org.s1ck.gdl.model.Vertex gdlVertex) {
    HashMap<String, String> properties = new HashMap<>();
    for (Map.Entry<String, Object> prop : gdlVertex.getProperties().entrySet()) {
      properties.put(prop.getKey(), prop.getValue().toString());
    }
    Set<Long> memberships = gdlVertex.getGraphs();
    String label = gdlVertex.getLabel();
    this.gei = new GraphElementInformation(label, properties, memberships);
  }

  public GraphElementInformation getGei() {
    return gei;
  }

  public void setGei(GraphElementInformation gei) {
    this.gei = gei;
  }

  @Override
  public String toString() {
    return String.format("(%s)",
        gei);
  }

  @Override
  public boolean equals(Object o) {
      if (this == o) {
          return true;
      }
      if (o == null || getClass() != o.getClass()) {
          return false;
      }
    Vertex vertex = (Vertex) o;
    return Objects.equals(gei, vertex.gei);
  }

  @Override
  public int hashCode() {
    return Objects.hash(gei);
  }
}
