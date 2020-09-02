package edu.leipzig.grafs.operators.matching.logic;

import static edu.leipzig.grafs.operators.matching.logic.ElementMatcher.matchesQueryElem;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import org.apache.flink.api.common.functions.FilterFunction;

public class EdgeQueryFilter implements FilterFunction<EdgeContainer> {

  private final QueryGraph queryGraph;

  public EdgeQueryFilter(final QueryGraph queryGraph) {
    this.queryGraph = queryGraph;
  }


  @Override
  public boolean filter(EdgeContainer edgeContainer) throws Exception {
    var edge = edgeContainer.getEdge();
    var source = edgeContainer.getSourceVertex();
    var target = edgeContainer.getTargetVertex();
    return filter(edge, source, target);
  }

  public boolean filter(Edge edge, Vertex source, Vertex target) {
    for (var queryEdge : queryGraph.getEdges()) {
      if (matchesQueryElem(queryEdge, edge)) {
        var querySource = queryGraph.getSourceForEdge(queryEdge);
        var queryTarget = queryGraph.getTargetForEdge(queryEdge);
        if (!matchesQueryElem(querySource, source)) {
          continue;
        }
        if (matchesQueryElem(queryTarget, target)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean filter(Edge edge) {
    for (var queryEdge : queryGraph.getEdges()) {
      if (matchesQueryElem(queryEdge, edge)) {
        return true;
      }
    }
    return false;
  }
}
