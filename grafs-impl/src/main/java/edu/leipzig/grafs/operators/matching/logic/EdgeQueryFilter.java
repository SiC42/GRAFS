package edu.leipzig.grafs.operators.matching.logic;


import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filters edges based on query graph.
 */
public class EdgeQueryFilter implements FilterFunction<EdgeContainer> {

  /**
   * Query graph which is used to filter edges
   */
  private final QueryGraph queryGraph;

  /**
   * Initializes the filter.
   *
   * @param queryGraph query graph which is used to filter edges
   */
  public EdgeQueryFilter(final QueryGraph queryGraph) {
    this.queryGraph = queryGraph;
  }

  /**
   * Returns <tt>true</tt> if for the given edge container there are matching source and target
   * vertex in the query graph and the corresponding edge also matches the given edge.
   *
   * @param edgeContainer Edge container which is tested for filtering
   * @return <tt>true</tt> if for the given edge container there are matching source and target
   * vertex in the query graph and the corresponding edge also matches the given edge
   */
  @Override
  public boolean filter(EdgeContainer edgeContainer) {
    var edge = edgeContainer.getEdge();
    var source = edgeContainer.getSourceVertex();
    var target = edgeContainer.getTargetVertex();
    return filter(edge, source, target);
  }

  /**
   * Returns <tt>true</tt> if for the given edge its source and target vertex have a match in the
   * query graph and the corresponding edge also matches the given edge.
   *
   * @param edge   edge which is tested for filtering
   * @param source source for given edge
   * @param target target for given edge
   * @return <tt>true</tt> if for the given edge container there are matching source and target
   * * vertex in the query graph and the corresponding edge also matches the given edge
   */
  public boolean filter(Edge edge, Vertex source, Vertex target) {
    for (var queryEdge : queryGraph.getEdges()) {
      if (ElementMatcher.matchesQueryElem(queryEdge, edge)) {
        var querySource = queryGraph.getSourceForEdge(queryEdge);
        var queryTarget = queryGraph.getTargetForEdge(queryEdge);
        if (!ElementMatcher.matchesQueryElem(querySource, source)) {
          continue;
        }
        if (ElementMatcher.matchesQueryElem(queryTarget, target)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Returns <tt>true</tt> if there is a corresponding edge in the query graph for the given edge.
   *
   * @param edge edge which is tested for filtering
   * @return <tt>true</tt> if there is a corresponding edge in the query graph for the given edge
   */
  public boolean filter(Edge edge) {
    for (var queryEdge : queryGraph.getEdges()) {
      if (ElementMatcher.matchesQueryElem(queryEdge, edge)) {
        return true;
      }
    }
    return false;
  }
}
