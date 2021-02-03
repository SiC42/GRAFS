package edu.leipzig.grafs.operators.matching.logic;

import edu.leipzig.grafs.model.BasicGraph;
import edu.leipzig.grafs.model.BasicTriplet;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.introspect.TypeResolutionContext.Basic;

/**
 * Filters vertices based on query graph.
 */
public class VertexQueryFilter implements FilterFunction<BasicTriplet<QueryVertex, ?>> {

  /**
   * Query graph which is used to filter edges
   */
  private final BasicGraph<QueryVertex, ?> queryGraph;

  /**
   * Initializes the filter.
   *
   * @param queryGraph query graph which is used to filter edges
   */
  public VertexQueryFilter(final BasicGraph<QueryVertex, ?> queryGraph) {
    this.queryGraph = queryGraph;
  }

  /**
   * Returns <tt>true</tt> if for the given triplet there are matching vertices in the query graph.
   *
   * @param triplet Triplet which is tested for filtering
   * @return <tt>true</tt> if for the given triplet there are matching vertices in the query
   * graph
   */
  @Override
  public boolean filter(BasicTriplet<QueryVertex, ?> triplet) throws Exception {
    var source = triplet.getSourceVertex();
    var target = triplet.getTargetVertex();
    boolean sourceInQuery = false;
    boolean targetInQuery = false;
    for (var queryVertex : queryGraph.getVertices()) {
      sourceInQuery = sourceInQuery || ElementMatcher.matchesQueryElem(queryVertex, source);
      targetInQuery = targetInQuery || ElementMatcher.matchesQueryElem(queryVertex, target);
      if (sourceInQuery && targetInQuery) {
        return true;
      }
    }
    return false;
  }
}
