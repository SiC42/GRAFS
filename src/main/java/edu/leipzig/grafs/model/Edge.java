package edu.leipzig.grafs.model;

import edu.leipzig.grafs.factory.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

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
    Edge reverseEdge = new EdgeFactory()
        .initEdge(this.getId(), this.getLabel(), this.getTargetId(), this.getSourceId(),
            this.getProperties(), this.getGraphIds());
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
