package streaming.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeStream;
import streaming.model.GraphElementInformation;
import streaming.operators.OperatorI;

public class Subgraph implements OperatorI {

  private final FilterFunction<GraphElementInformation> vertexGeiPredicate;
  private final FilterFunction<GraphElementInformation> edgeGeiPredicate;
  private final Strategy strategy;


  /**
   * Available execution strategies.
   */
  public enum Strategy {
    /**
     * Applies both filter functions on the input vertex and edge data set.
     */
    BOTH,
    /**
     * Only applies the vertex filter function and adds the incident edges connecting those vertices
     * via a join.
     */
    VERTEX_INDUCED,
    /**
     * Only applies the edge filter function and computes the resulting vertices via:<br> {@code (E
     * |><| V ON e.source = v.id) U (E |><| V on e.target = v.id)}
     */
    EDGE_INDUCED
  }

  public Subgraph(
      FilterFunction<GraphElementInformation> vertexGeiPredicate,
      FilterFunction<GraphElementInformation> edgeGeiPredicate,
      Strategy strategy) {
    if (strategy == Strategy.BOTH &&
        (vertexGeiPredicate == null || edgeGeiPredicate == null)) {
      throw new IllegalArgumentException("No vertex or no edge filter function was given.");
    }

    if (strategy == Strategy.VERTEX_INDUCED && vertexGeiPredicate == null) {
      throw new IllegalArgumentException("No vertex filter functions was given.");
    }

    if ((strategy == Strategy.EDGE_INDUCED || strategy == Strategy.EDGE_INDUCED_PROJECT_FIRST) &&
        edgeGeiPredicate == null) {
      throw new IllegalArgumentException("No vertex edge functions was given.");
    }

    this.strategy = strategy;

    this.vertexGeiPredicate = vertexGeiPredicate;
    this.edgeGeiPredicate = edgeGeiPredicate;
  }


  private DataStream<Edge> vertexInducedSubgraph(DataStream<Edge> es) {
    FilterFunction<Edge> edgeFilter = edge ->
        vertexGeiPredicate.filter(edge.getSource().getGei()) && vertexGeiPredicate
            .filter(edge.getTarget().getGei());

    return applyFilterToStream(es, edgeFilter);
  }

  private DataStream<Edge> edgeInducedSubgraph(DataStream<Edge> es) {
    FilterFunction<Edge> edgeFilter = edge -> edgeGeiPredicate.filter(edge.getGei());
    return applyFilterToStream(es, edgeFilter);
  }

  private DataStream<Edge> subgraph(DataStream<Edge> es) {
    FilterFunction<Edge> edgeFilter = edge ->
        edgeGeiPredicate.filter(edge.getGei()) &&
        vertexGeiPredicate.filter(edge.getSource().getGei()) &&
        vertexGeiPredicate.filter(edge.getTarget().getGei());
    return applyFilterToStream(es, edgeFilter);
  }

  private DataStream<Edge> applyFilterToStream(DataStream<Edge> es,
      FilterFunction<Edge> edgeFilter) {
    DataStream<Edge> filteredStream = es.filter(edgeFilter);
    return filteredStream;
  }

  @Override
  public EdgeStream execute(DataStream<Edge> stream) {
    DataStream<Edge> filteredStream;
    switch (strategy) {
      case BOTH:
        filteredStream = subgraph(stream);
        break;
      case VERTEX_INDUCED:
        filteredStream = vertexInducedSubgraph(stream);
        break;
      case EDGE_INDUCED:
        filteredStream = edgeInducedSubgraph(stream);
        break;
      default:
        throw new IllegalArgumentException("Strategy " + strategy + " is not supported");
    }
    return new EdgeStream(filteredStream);
  }

}
