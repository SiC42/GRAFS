package edu.leipzig.grafs.operators.matching.model;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import java.util.HashSet;
import java.util.Set;

public class QueryGraph extends Graph {

  public QueryGraph() {
    super(new HashSet<>(), new HashSet<>());
  }

  public QueryGraph(Set<Vertex> vertices, Set<Edge> edges) {
    super(vertices, edges);
  }

  public static QueryGraph fromGraph(Graph graph) {
    return new QueryGraph(graph.getVertices(), graph.getEdges());
  }

  public boolean isVertexOnly() {
    return edges.isEmpty();
  }
}
