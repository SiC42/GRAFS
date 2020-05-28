package streaming.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

public class Vertex extends GraphElement {


  public Vertex() {
    super();
  }

  public Vertex(Element element){
    super(element);
  }


  /**
   * Creates a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex label
   * @param properties vertex properties
   * @param graphs     graphs that vertex is contained in
   */
  public Vertex(final GradoopId id, final String label,
      final Map<String, String> properties, final GradoopIdSet graphs) {
    super(id, label, properties, graphs);
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
