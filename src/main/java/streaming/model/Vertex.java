package streaming.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Vertex extends Element {


  public Vertex() {
    super();
  }


  public Vertex(Element gei) {
    super(gei);
  }

  public Vertex(org.s1ck.gdl.model.Vertex gdlVertex) {
    HashMap<String, String> properties = new HashMap<>();
    for (Map.Entry<String, Object> prop : gdlVertex.getProperties().entrySet()) {
      properties.put(prop.getKey(), prop.getValue().toString());
    }
    Set<Long> memberships = gdlVertex.getGraphs();
    String label = gdlVertex.getLabel();
  }


  @Override
  public String toString() {
    return String.format("(%s)",
        super.toString());
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
    return Objects.equals(getId(), vertex.getId());
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }
}
