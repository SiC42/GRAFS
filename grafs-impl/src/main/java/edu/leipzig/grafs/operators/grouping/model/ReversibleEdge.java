package edu.leipzig.grafs.operators.grouping.model;

import edu.leipzig.grafs.model.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

public class ReversibleEdge extends Edge {

  private final boolean reverse;

  /**
   * Creates an edge with the given information
   *
   * @param id         ID for the edge
   * @param label      label for the edge
   * @param sourceId   ID of the source vertex for the edge
   * @param targetId   ID of the target vertex for the edge
   * @param properties properties for the edge
   * @param graphIds   graph ids for the edge
   */
  private ReversibleEdge(boolean reverse, GradoopId id, String label, GradoopId sourceId, GradoopId targetId,
      Properties properties,
      GradoopIdSet graphIds) {
    super(id, label, sourceId, targetId, properties, graphIds);
    this.reverse = reverse;
  }


  public static ReversibleEdge create(Edge e, boolean reverse) {
    ReversibleEdge revEdge;
    if (reverse) {
      revEdge = new ReversibleEdge(reverse, e.getId(), e.getLabel(), e.getTargetId(), e.getSourceId(),
          e.getProperties(),
          e.getGraphIds());
    } else {
      revEdge = new ReversibleEdge(reverse, e.getId(), e.getLabel(), e.getSourceId(), e.getTargetId(),
          e.getProperties(),
          e.getGraphIds());
    }
    return revEdge;
  }

  public Edge toEdge() {
    return new Edge(this.getId(), this.getLabel(), this.getSourceId(), this.getTargetId(),
        this.getProperties(), this.getGraphIds());
  }

  public boolean isReverse(){
    return reverse;
  }

  @Override
  public String toString() {
    return String.format("[%s, reverse=%b]",
        super.toString(),
        reverse);
  }

}
