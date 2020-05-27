package streaming.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Element;
import streaming.model.Vertex;
import streaming.operators.OperatorI;

public class Subgraph implements OperatorI {

  private final FilterFunction<Vertex> vertexGeiPredicate;
  private final FilterFunction<Edge> edgeGeiPredicate;
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
      final FilterFunction<Vertex> vertexGeiPredicate,
      final FilterFunction<Edge> edgeGeiPredicate,
      final Strategy strategy) {
    if (strategy == Strategy.BOTH &&
        (vertexGeiPredicate == null || edgeGeiPredicate == null)) {
      throw new IllegalArgumentException("No vertex or no edge filter function was given.");
    }

    if (strategy == Strategy.VERTEX_INDUCED && vertexGeiPredicate == null) {
      throw new IllegalArgumentException("No vertex filter functions was given.");
    }

    if ((strategy == Strategy.EDGE_INDUCED) &&
        edgeGeiPredicate == null) {
      throw new IllegalArgumentException("No vertex edge functions was given.");
    }

    this.strategy = strategy;

    this.vertexGeiPredicate = vertexGeiPredicate;
    this.edgeGeiPredicate = edgeGeiPredicate;
  }


  private DataStream<EdgeContainer> vertexInducedSubgraph(DataStream<EdgeContainer> es) {
    FilterFunction<EdgeContainer> edgeFilter = ec ->
        vertexGeiPredicate.filter(ec.getSourceVertex()) && vertexGeiPredicate
            .filter(ec.getTargetVertex());
    return es.filter(edgeFilter);
  }

  private DataStream<EdgeContainer> edgeInducedSubgraph(DataStream<EdgeContainer> es) {
    FilterFunction<EdgeContainer> edgeFilter = edge -> edgeGeiPredicate.filter(edge.getEdge());
    return es.filter(edgeFilter);
  }

  private DataStream<EdgeContainer> subgraph(DataStream<EdgeContainer> es) {
    FilterFunction<EdgeContainer> filter = ec ->
        edgeGeiPredicate.filter(ec.getEdge()) &&
        vertexGeiPredicate.filter(ec.getSourceVertex()) &&
        vertexGeiPredicate.filter(ec.getTargetVertex());
    return es.filter(filter);
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    DataStream<EdgeContainer> filteredStream;
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
    return filteredStream;
  }

}
