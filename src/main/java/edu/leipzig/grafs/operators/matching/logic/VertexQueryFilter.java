package edu.leipzig.grafs.operators.matching.logic;

import static edu.leipzig.grafs.operators.matching.logic.ElementMatcher.matchesQueryElem;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import org.apache.flink.api.common.functions.FilterFunction;

public class VertexQueryFilter implements FilterFunction<EdgeContainer> {

  private final QueryGraph queryGraph;

  public VertexQueryFilter(final QueryGraph queryGraph) {
    this.queryGraph = queryGraph;
  }

  @Override
  public boolean filter(EdgeContainer edgeContainer) throws Exception {
    var source = edgeContainer.getSourceVertex();
    var target = edgeContainer.getTargetVertex();
    boolean sourceInQuery = false;
    boolean targetInQuery = false;
    for (var queryVertex : queryGraph.getVertices()) {
      sourceInQuery = sourceInQuery || matchesQueryElem(queryVertex, source);
      targetInQuery = targetInQuery || matchesQueryElem(queryVertex, target);
      if (sourceInQuery && targetInQuery) {
        return true;
      }
    }
    return false;
  }
}
