package streaming.model;

public class EdgeContainer {

  private Edge edge;
  private Vertex sourceVertex;
  private Vertex targetVertex;


  public EdgeContainer(Edge edge, Vertex sourceVertex, Vertex targetVertex) {
    this.edge = edge;
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
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

}
