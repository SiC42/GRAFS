package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Abstract class that provides basic functionalities for aggregating on vertices in an edge
 * stream.
 *
 * @param <W> the type of window to be used for the grouping
 */
abstract class VertexAggregationProcess<W extends Window> extends
    ElementAggregationProcess<W> {


  /**
   * Determines if the give vertex was already used for aggregating on the aggregated vertex. If not
   * the aggregate functions are applied.
   *
   * @param aggregatedVertex   vertex on which the aggregates should be set
   * @param curVertex          vertex used to determine the new aggregate
   * @param aggregateFunctions aggregate functions to be used in the aggregation process
   * @return the aggregation vertex with the new aggregates
   */
  AggregatedVertex aggregateVertex(AggregatedVertex aggregatedVertex,
      Vertex curVertex, Set<AggregateFunction> aggregateFunctions) {
    if (aggregatedVertex.isAlreadyAggregated(curVertex)) {
      return aggregatedVertex;
    } else {
      aggregatedVertex.addVertex(curVertex);
      return (AggregatedVertex) aggregateElement(aggregatedVertex, curVertex,
          aggregateFunctions
      );
    }
  }

}
