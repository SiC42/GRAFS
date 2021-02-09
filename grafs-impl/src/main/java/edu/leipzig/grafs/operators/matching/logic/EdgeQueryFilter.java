package edu.leipzig.grafs.operators.matching.logic;


import edu.leipzig.grafs.model.BasicGraph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filters edges based on query graph.
 */
public class EdgeQueryFilter implements FilterFunction<Triplet<QueryVertex, QueryEdge>> {

  /**
   * Query graph which is used to filter edges
   */
  private final BasicGraph<QueryVertex, QueryEdge> queryGraph;

  /**
   * Initializes the filter.
   *
   * @param queryGraph query graph which is used to filter edges
   */
  public EdgeQueryFilter(final BasicGraph<QueryVertex, QueryEdge> queryGraph) {
    this.queryGraph = queryGraph;
  }

  /**
   * Returns <tt>true</tt> if for the given triplet there are matching source and target vertex in
   * the query graph and the corresponding edge also matches the given edge.
   *
   * @param triplet Triplet which is tested for filtering
   * @return <tt>true</tt> if for the given triplet there are matching source and target
   * vertex in the query graph and the corresponding edge also matches the given edge
   */
  @Override
  public boolean filter(Triplet<QueryVertex, QueryEdge> triplet) {
    var edge = triplet.getEdge();
    var source = triplet.getSourceVertex();
    var target = triplet.getTargetVertex();
    return filter(edge, source, target);
  }

  /**
   * Returns <tt>true</tt> if for the given edge its source and target vertex have a match in the
   * query graph and the corresponding edge also matches the given edge.
   *
   * @param edge   edge which is tested for filtering
   * @param source source for given edge
   * @param target target for given edge
   * @return <tt>true</tt> if for the given triplet there are matching source and target
   * * vertex in the query graph and the corresponding edge also matches the given edge
   */
  public boolean filter(QueryEdge edge, QueryVertex source, QueryVertex target) {
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
  public boolean filter(QueryEdge edge) {
    for (var queryEdge : queryGraph.getEdges()) {
      if (ElementMatcher.matchesQueryElem(queryEdge, edge)) {
        return true;
      }
    }
    return false;
  }
}
