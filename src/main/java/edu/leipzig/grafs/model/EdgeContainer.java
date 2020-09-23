package edu.leipzig.grafs.model;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;

public class EdgeContainer {

  private final Edge edge;
  private final Vertex sourceVertex;
  private final Vertex targetVertex;


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

  public void addGraphId(GradoopId graphId) {
    this.edge.addGraphId(graphId);
    this.sourceVertex.addGraphId(graphId);
    this.targetVertex.addGraphId(graphId);
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
