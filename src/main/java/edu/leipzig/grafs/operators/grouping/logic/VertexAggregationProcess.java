package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;

abstract class VertexAggregationProcess<W extends Window> extends
    GraphElementAggregationProcess<W> {

  protected AggregatedVertex aggregateVertex(Set<AggregateFunction> aggregateFunctions,
      AggregatedVertex aggregatedVertex,
      Vertex curVertex) {
    if (aggregatedVertex.isAlreadyAggregated(curVertex)) {
      return aggregatedVertex;
    } else {
      aggregatedVertex.addVertex(curVertex);
      return (AggregatedVertex) aggregateGraphElement(aggregatedVertex, curVertex,
          aggregateFunctions
      );
    }
  }

}
