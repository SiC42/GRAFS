package streaming.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

public class Edge extends GraphElement {

  private GradoopId sourceId;
  private GradoopId targetId;
  private boolean reverse;

  public Edge() {
    this(null, null);
  }

  public Edge(Edge otherEdge) {
    super(otherEdge);
    sourceId = otherEdge.getSourceId();
    targetId = otherEdge.getTargetId();
  }

  public Edge(Element element, GradoopId sourceId, GradoopId targetId) {
    super(element);
    this.sourceId = sourceId;
    this.targetId = targetId;
    this.reverse = false;
  }


  public Edge(GradoopId sourceId, GradoopId targetId) {
    super();
    this.sourceId = sourceId;
    this.targetId = targetId;
    this.reverse = false;
  }

  public Edge(GradoopId id, String label, GradoopId sourceId, GradoopId targetId,
      Map<String, String> properties,
      GradoopIdSet graphIds) {
    super(id, label, properties, graphIds);
    this.sourceId = sourceId;
    this.targetId = targetId;
  }

  public Edge(org.s1ck.gdl.model.Edge gdlEdge, org.s1ck.gdl.model.Vertex gdlSourceV,
      org.s1ck.gdl.model.Vertex gdlTargetV) {
    HashMap<String, String> properties = new HashMap<>();
    for (Map.Entry<String, Object> prop : gdlEdge.getProperties().entrySet()) {
      properties.put(prop.getKey(), prop.getValue().toString());
    }
    Set<Long> memberships = gdlEdge.getGraphs();
    String label = gdlEdge.getLabel();
  }


  public GradoopId getSourceId() {
    return sourceId;
  }

  public void setSourceId(GradoopId newSource) {
    sourceId = newSource;
  }

  public GradoopId getTargetId() {
    return targetId;
  }

  public void setTargetId(GradoopId newTarget) {
    targetId = newTarget;
  }

  private void setReverse(boolean isReverse){
    reverse = isReverse;
  }

  public boolean isReverse() {
    return reverse;
  }

  public Edge createReverseEdge() {
    Edge reverseEdge = new Edge(this.getTargetId(), this.getSourceId());
    reverseEdge.setReverse(true);
    return reverseEdge;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Edge edge = (Edge) o;
    return Objects.equals(sourceId, edge.sourceId) &&
        Objects.equals(targetId, edge.targetId);
  }

  @Override
  public String toString() {
    return String.format("[%s, reverse=%b]",
        super.toString(),
        reverse);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId());
  }
}
