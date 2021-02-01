package edu.leipzig.grafs.model;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Data model that encapsulates {@link Edge} and its {@link Vertex}. Used for separation purposes
 * (e.g. an edge shouldn't be modified in vertex transformations and vice versa).
 */
public class Triplet extends BasicTriplet<Vertex, Edge> implements Serializable {


  /**
   * Empty constructor used for serialization.
   */
  protected Triplet() {
  }

  /**
   * Constructs an <tt>Triplet</tt> with the given information.
   *
   * @param edge         edge information
   * @param sourceVertex source vertex of the given edge
   * @param targetVertex target vertex of the given edge
   * @throws RuntimeException when the IDs of the given vertices do not match with the corresponding
   *                          IDs in the edge
   */
  public Triplet(Edge edge, Vertex sourceVertex, Vertex targetVertex)
      throws RuntimeException {
    super(edge,sourceVertex,targetVertex);
  }

  /**
   * Constructs a <tt>Triplet</tt> with the given information. Here, the edge is constructed using
   * the ids in <tt>sourceVertex</tt> and <tt>targetVertex</tt> parameter
   *
   * @param prevEdge     edge information
   * @param sourceVertex source vertex of the given edge
   * @param targetVertex target vertex of the given edge
   */
  public static Triplet createTriplet(GraphElement prevEdge, GraphElement sourceVertex,
      GraphElement targetVertex) {
    var source = VertexFactory.createVertex(
        sourceVertex.getLabel(),
        sourceVertex.getProperties(),
        sourceVertex.getGraphIds());
    var target = VertexFactory.createVertex(
        targetVertex.getLabel(),
        targetVertex.getProperties(),
        targetVertex.getGraphIds());
    var e = EdgeFactory.createEdge(
        prevEdge.getLabel(),
        sourceVertex.getId(),
        targetVertex.getId(),
        prevEdge.getProperties(),
        prevEdge.getGraphIds());
    return new Triplet(e,source,target);
  }





  /**
   * Creates a copy of this <tt>Triplet</tt>, but with source and target vertex reversed and the
   * appropriate flags in edge set.
   */
  public Triplet createReverseTriplet() {
    var reverseEdge = getEdge().createReverseEdge();
    return new Triplet(reverseEdge, getTargetVertex(), getSourceVertex());
  }

}
