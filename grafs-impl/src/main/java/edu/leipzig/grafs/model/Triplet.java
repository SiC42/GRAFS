package edu.leipzig.grafs.model;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.operators.grouping.model.ReversableEdge;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Data model that encapsulates {@link Edge} and its {@link Vertex}. Used for separation purposes
 * (e.g. an edge shouldn't be modified in vertex transformations and vice versa).
 */
public class Triplet<V extends Vertex, E extends Edge> implements Serializable {

  private E edge;
  private V sourceVertex;
  private V targetVertex;

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
  public Triplet(E edge, V sourceVertex, V targetVertex)
      throws RuntimeException {
    checkIfIdsMatch(edge, sourceVertex, targetVertex);
    this.edge = edge;
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
  }

  /**
   * Constructs a <tt>Triplet</tt> with the given information. Here, the edge is constructed using
   * the ids in <tt>sourceVertex</tt> and <tt>targetVertex</tt> parameter
   *
   * @param prevEdge     edge information
   * @param sourceVertex source vertex of the given edge
   * @param targetVertex target vertex of the given edge
   */
  public static Triplet<Vertex, Edge> createTriplet(GraphElement prevEdge,
      GraphElement sourceVertex,
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
    return new Triplet<>(e, source, target);
  }

  private void checkIfIdsMatch(E edge, V sourceVertex, V targetVertex) {
    if (edge == null) {
      return;
    }
    if (sourceVertex != null && !sourceVertex.getId().equals(edge.getSourceId())) {
      throw new RuntimeException(
          "ID of provided source vertex does not match with source id in provided edge.");
    }
    if (targetVertex != null && !targetVertex.getId().equals(edge.getTargetId())) {
      throw new RuntimeException(
          "ID of provided target vertex does not match with target id in provided edge.");
    }
  }

  /**
   * Returns edge of this triplet.
   *
   * @return edge of this triplet
   */
  public E getEdge() {
    return edge;
  }


  protected void setEdge(E edge) {
    checkIfIdsMatch(edge, sourceVertex, targetVertex);
    this.edge = edge;
  }

  /**
   * Returns source vertex of this triplet.
   *
   * @return source vertex of this triplet
   */
  public V getSourceVertex() {
    return sourceVertex;
  }

  public void setSourceVertex(V sourceVertex) {
    checkIfIdsMatch(edge, sourceVertex, targetVertex);
    this.sourceVertex = sourceVertex;
  }

  /**
   * Returns target vertex of this triplet.
   *
   * @return target vertex of this triplet
   */
  public V getTargetVertex() {
    return targetVertex;
  }

  public void setTargetVertex(V targetVertex) {
    checkIfIdsMatch(edge, sourceVertex, targetVertex);
    this.targetVertex = targetVertex;
  }

  /**
   * Adds the given graph ID to the edge, source and target vertex.
   *
   * @param graphId graph ID that should be added to the elements
   */
  public void addGraphId(GradoopId graphId) {
    this.edge.addGraphId(graphId);
    this.sourceVertex.addGraphId(graphId);
    this.targetVertex.addGraphId(graphId);
  }

  /**
   * Creates a copy of this <tt>Triplet</tt>, but with source and target vertex reversed and the
   * appropriate flags in edge set.
   */
  public Triplet<V, ReversableEdge> createReverseTriplet() {
    var reverseEdge = ReversableEdge.create(edge, true);
    return new Triplet<>(reverseEdge, targetVertex, sourceVertex);
  }

  @Override
  public String toString() {
    return String.format("%s-%s->%s", sourceVertex, edge, targetVertex);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Triplet<?, ?> that = (Triplet<?, ?>) o;
    return Objects.equals(edge, that.edge) &&
        Objects.equals(sourceVertex, that.sourceVertex) &&
        Objects.equals(targetVertex, that.targetVertex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(edge, sourceVertex, targetVertex);
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeObject(edge);
    out.writeObject(sourceVertex);
    out.writeObject(targetVertex);
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    this.edge = (E) in.readObject();
    this.sourceVertex = (V) in.readObject();
    this.targetVertex = (V) in.readObject();

  }
}
