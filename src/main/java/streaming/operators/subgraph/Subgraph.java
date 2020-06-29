package streaming.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Vertex;
import streaming.operators.OperatorI;

public class Subgraph implements OperatorI {

  private final FilterFunction<Vertex> vertexFilter;
  private final FilterFunction<Edge> edgeFilter;
  private final Strategy strategy;


  public Subgraph(
      final FilterFunction<Vertex> vertexFilter,
      final FilterFunction<Edge> edgeFilter,
      final Strategy strategy) {
    if (strategy == Strategy.BOTH &&
        (vertexFilter == null || edgeFilter == null)) {
      throw new IllegalArgumentException("No vertex or no edge filter function was given.");
    }

    if (strategy == Strategy.VERTEX_INDUCED && vertexFilter == null) {
      throw new IllegalArgumentException("No vertex filter functions was given.");
    }

    if ((strategy == Strategy.EDGE_INDUCED) &&
        edgeFilter == null) {
      throw new IllegalArgumentException("No vertex edge functions was given.");
    }

    this.strategy = strategy;

    this.vertexFilter = vertexFilter;
    this.edgeFilter = edgeFilter;
  }

  private DataStream<EdgeContainer> vertexInducedSubgraph(DataStream<EdgeContainer> ecStream) {
    FilterFunction<EdgeContainer> ecFilter = ec ->
        vertexFilter.filter(ec.getSourceVertex()) && vertexFilter
            .filter(ec.getTargetVertex());
    return ecStream.filter(ecFilter);
  }

  private DataStream<EdgeContainer> edgeInducedSubgraph(DataStream<EdgeContainer> ecStream) {
    FilterFunction<EdgeContainer> ecFilter = ec -> edgeFilter.filter(ec.getEdge());
    return ecStream.filter(ecFilter);
  }

  private DataStream<EdgeContainer> subgraph(DataStream<EdgeContainer> ecStream) {
    FilterFunction<EdgeContainer> ecFilter = ec ->
        edgeFilter.filter(ec.getEdge()) &&
            vertexFilter.filter(ec.getSourceVertex()) &&
            vertexFilter.filter(ec.getTargetVertex());
    return ecStream.filter(ecFilter);
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    DataStream<EdgeContainer> filteredStream;
    switch (strategy) {
      case BOTH:
        return subgraph(stream);
      case VERTEX_INDUCED:
        return vertexInducedSubgraph(stream);
      case EDGE_INDUCED:
        return edgeInducedSubgraph(stream);
      default:
        throw new IllegalArgumentException("Strategy " + strategy + " is not supported");
    }
  }

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

}
