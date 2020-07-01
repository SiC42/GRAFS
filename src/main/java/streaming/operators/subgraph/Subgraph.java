package streaming.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Vertex;
import streaming.operators.OperatorI;

public class Subgraph implements OperatorI {

  private final FilterFunction<EdgeContainer> ecFilter;


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

    switch (strategy) {
      case BOTH:
        ecFilter = createSubGraphFilter(vertexFilter, edgeFilter);
        break;
      case VERTEX_INDUCED:
        ecFilter = createVertexInducedSubgraphFilter(vertexFilter);
        break;
      case EDGE_INDUCED:
        ecFilter = createEdgeInducedSubgraphFilter(edgeFilter);
        break;
      default:
        throw new IllegalArgumentException("Strategy " + strategy + " is not supported");
    }
  }
  
  private FilterFunction<EdgeContainer> createSubGraphFilter(FilterFunction<Vertex> vertexFilter,
      FilterFunction<Edge> edgeFilter) {
    FilterFunction<EdgeContainer> ecFilter = ec ->
        edgeFilter.filter(ec.getEdge()) &&
            vertexFilter.filter(ec.getSourceVertex()) &&
            vertexFilter.filter(ec.getTargetVertex());
    return ecFilter;
  }

  private FilterFunction<EdgeContainer> createVertexInducedSubgraphFilter(
      FilterFunction<Vertex> vertexFilter) {
    FilterFunction<EdgeContainer> ecFilter = ec ->
        vertexFilter.filter(ec.getSourceVertex()) && vertexFilter
            .filter(ec.getTargetVertex());
    return ecFilter;
  }

  private FilterFunction<EdgeContainer> createEdgeInducedSubgraphFilter(
      FilterFunction<Edge> edgeFilter) {
    FilterFunction<EdgeContainer> ecFilter = ec -> edgeFilter.filter(ec.getEdge());
    return ecFilter;
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return stream.filter(ecFilter);
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
