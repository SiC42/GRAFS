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

  public EdgeContainer(Element edgeElement, Element sourceVertex, Element targetVertex){
    this.sourceVertex = new Vertex(sourceVertex);
    this.targetVertex = new Vertex(targetVertex);
    this.edge = new Edge(edgeElement, sourceVertex.getId(), targetVertex.getId());
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

}
