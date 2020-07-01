package streaming.model;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import streaming.factory.EdgeFactory;

public class Edge extends GraphElement {

  private GradoopId sourceId;
  private GradoopId targetId;
  private boolean reverse;

  public Edge() {

  }

  public Edge(GradoopId id, String label, GradoopId sourceId, GradoopId targetId,
      Properties properties,
      GradoopIdSet graphIds) {
    super(id, label, properties, graphIds);
    this.sourceId = sourceId;
    this.targetId = targetId;
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

  private void setReverse() {
    reverse = true;
  }

  public boolean isReverse() {
    return reverse;
  }

  public Edge createReverseEdge() {
    Edge reverseEdge = new EdgeFactory().createEdge(this.getTargetId(), this.getSourceId());
    reverseEdge.setReverse();
    return reverseEdge;
  }

  @Override
  public String toString() {
    return String.format("[%s, reverse=%b]",
        super.toString(),
        reverse);
  }

}
