package edu.leipzig.grafs.model;

import edu.leipzig.grafs.exceptions.VerticesAlreadySetException;
import edu.leipzig.grafs.factory.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Data model that represents the edge in the property graph model (with graph membership).
 */
public class Edge extends GraphElement {

  private GradoopId sourceId;
  private GradoopId targetId;
  private boolean reverse;


  /**
   * Empty constructor for flink serialization.
   */
  protected Edge() {
    super();
  }

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
  public Edge(GradoopId id, String label, GradoopId sourceId, GradoopId targetId,
      Properties properties,
      GradoopIdSet graphIds) {
    super(id, label, properties, graphIds);
    this.sourceId = sourceId;
    this.targetId = targetId;
  }

  /**
   * Returns ID of the source vertex.
   *
   * @return ID of the source vertex
   */
  public GradoopId getSourceId() {
    return sourceId;
  }

  /**
   * Sets the source vertex ID for this edge. Only use this if the ID was not set yet.
   *
   * @param sourceId source vertex ID for this edge.
   * @throws VerticesAlreadySetException if the source ID is already set. Make a new edge instead.
   */
  public void setSourceId(GradoopId sourceId) throws VerticesAlreadySetException {
    if (this.sourceId != null) {
      throw new VerticesAlreadySetException(
          "Source vertex ID is already set. Make a new edge instead.");
    }
    this.sourceId = sourceId;
  }

  /**
   * Returns ID of the target vertex.
   *
   * @return ID of the target vertex
   */
  public GradoopId getTargetId() {
    return targetId;
  }

  /**
   * Sets the target vertex ID for this edge. Only use this if the ID was not set yet.
   *
   * @param targetId target vertex ID for this edge.
   * @throws VerticesAlreadySetException if the target ID is already set. Make a new edge instead.
   */
  public void setTargetId(GradoopId targetId) throws VerticesAlreadySetException {
    if (this.targetId != null) {
      throw new VerticesAlreadySetException(
          "Target vertex ID is already set. Make a new edge instead.");
    }
    this.targetId = targetId;
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
   * Create a copy of this edge, with source and target vertex reversed and the appropriate flag
   * set.
   *
   * @return "reverse" edge, with source and target vertex reversed and the appropriate flag set.
   */
  public Edge createReverseEdge() {
    Edge reverseEdge = EdgeFactory
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
