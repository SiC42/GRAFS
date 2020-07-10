package edu.leipzig.grafs.model;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import java.util.Objects;

public class EdgeContainer {

  private Edge edge;
  private Vertex sourceVertex;
  private Vertex targetVertex;


  public EdgeContainer(Edge edge, Vertex sourceVertex, Vertex targetVertex) {
    this.edge = edge;
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
  }

  public EdgeContainer(GraphElement prevEdge, GraphElement sourceVertex,
      GraphElement targetVertex) {
    this.sourceVertex = new VertexFactory().createVertex(
        sourceVertex.getLabel(),
        sourceVertex.getProperties(),
        sourceVertex.getGraphIds());
    this.targetVertex = new VertexFactory().createVertex(
        targetVertex.getLabel(),
        targetVertex.getProperties(),
        targetVertex.getGraphIds());
    this.edge = new EdgeFactory().createEdge(
        prevEdge.getLabel(),
        sourceVertex.getId(),
        targetVertex.getId(),
        prevEdge.getProperties(),
        prevEdge.getGraphIds());
  }

  public Edge getEdge() {
    return edge;
  }

  public void setEdge(Edge edge) {
    this.edge = edge;
  }

  public Vertex getSourceVertex() {
    return sourceVertex;
  }

  public void setSourceVertex(Vertex sourceVertex) {
    this.sourceVertex = sourceVertex;
  }

  public Vertex getTargetVertex() {
    return targetVertex;
  }

  public void setTargetVertex(Vertex targetVertex) {
    this.targetVertex = targetVertex;
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
}
