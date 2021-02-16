package edu.leipzig.grafs.operators.grouping.model;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

public class ReversableEdge extends Edge {

  private boolean reverse;

  public ReversableEdge(String label,
      GradoopId sourceId,
      GradoopId targetId,
      Properties properties,
      GradoopIdSet graphIds) {
    super(GradoopId.get(), label, sourceId, targetId, properties, graphIds);
  }

  public ReversableEdge(GradoopId id, String label,
      GradoopId sourceId,
      GradoopId targetId,
      Properties properties,
      GradoopIdSet graphIds) {
    super(id, label, sourceId, targetId, properties, graphIds);
  }

  /**
   * Marks this edge as reverse. Reverse edges are used for certain parameters, where we group by
   * the vertice in a edge, as we can't group them otherwise
   */
  private void setReverse() {
    reverse = true;
  }

  /**
   * Returns true if this edge is marked as reverse.
   *
   * @return true if this edge is marked as reverse
   */
  public boolean isReverse() {
    return reverse;
  }

  /**
   * Create a copy of given edge, with source and target vertex reversed and the appropriate flag
   * set.
   *
   * @return "reverse" edge, with source and target vertex reversed and the appropriate flag set.
   */
  public static ReversableEdge createReverseEdge(Edge e) {
    var reverseEdge = new ReversableEdge(e.getId(), e.getLabel(), e.getTargetId(), e.getSourceId(),
        e.getProperties(), e.getGraphIds());
    reverseEdge.setReverse();
    return reverseEdge;
  }

  /**
   * Create a copy of given edge as reversable edge
   *
   * @return "reverse" edge, with source and target vertex reversed and the appropriate flag set.
   */
  public static ReversableEdge createNormalEdge(Edge e) {
    return new ReversableEdge(e.getId(), e.getLabel(), e.getSourceId(), e.getTargetId(),
        e.getProperties(), e.getGraphIds());
  }

  public Edge toNormalEdge(){
    return EdgeFactory.createEdge(this);
  }

}
