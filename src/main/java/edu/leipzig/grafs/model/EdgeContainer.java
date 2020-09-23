package edu.leipzig.grafs.model;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

public class EdgeContainer implements Serializable {

  private static final long serialVersionUID = 547531514201914518L;
  private Edge edge;
  private Vertex sourceVertex;
  private Vertex targetVertex;

  protected EdgeContainer() {

  }


  public EdgeContainer(Edge edge, Vertex sourceVertex, Vertex targetVertex) {
    if (!sourceVertex.getId().equals(edge.getSourceId())) {
      throw new RuntimeException(
          "ID of provided source vertex does not match with source id in provided edge.");
    }
    if (!targetVertex.getId().equals(edge.getTargetId())) {
      throw new RuntimeException(
          "ID of provided target vertex does not match with target id in provided edge.");
    }
    this.edge = edge;
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
  }

  public EdgeContainer(GraphElement prevEdge, GraphElement sourceVertex,
      GraphElement targetVertex) {
    this.sourceVertex = VertexFactory.createVertex(
        sourceVertex.getLabel(),
        sourceVertex.getProperties(),
        sourceVertex.getGraphIds());
    this.targetVertex = VertexFactory.createVertex(
        targetVertex.getLabel(),
        targetVertex.getProperties(),
        targetVertex.getGraphIds());
    this.edge = EdgeFactory.createEdge(
        prevEdge.getLabel(),
        sourceVertex.getId(),
        targetVertex.getId(),
        prevEdge.getProperties(),
        prevEdge.getGraphIds());
  }

  public Edge getEdge() {
    return edge;
  }

  public Vertex getSourceVertex() {
    return sourceVertex;
  }

  public Vertex getTargetVertex() {
    return targetVertex;
  }

  public EdgeContainer createReverseEdgeContainer() {
    Edge reverseEdge = this.edge.createReverseEdge();
    return new EdgeContainer(reverseEdge, targetVertex, sourceVertex);
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
    EdgeContainer that = (EdgeContainer) o;
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
    this.edge = (Edge) in.readObject();
    this.sourceVertex = (Vertex) in.readObject();
    this.targetVertex = (Vertex) in.readObject();

  }
}
