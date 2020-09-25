package edu.leipzig.grafs.benchmarking.subgraph;

import edu.leipzig.grafs.benchmarking.functions.FilterFunctionWithMeter;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.subgraph.Subgraph;
import org.apache.flink.api.common.functions.FilterFunction;

public class BenchmarkSubgraph extends Subgraph {

  public BenchmarkSubgraph(
      FilterFunction<Vertex> vertexFilter,
      FilterFunction<Edge> edgeFilter,
      Strategy strategy) {
    this(vertexFilter, edgeFilter, strategy, "subgraphMeter");
  }

  public BenchmarkSubgraph(
      FilterFunction<Vertex> vertexFilter,
      FilterFunction<Edge> edgeFilter,
      Strategy strategy,
      String meterName) {
    super(vertexFilter, edgeFilter, strategy);
    this.ecFilter = new FilterFunctionWithMeter<EdgeContainer>(meterName) {
      @Override
      public boolean plainFilter(EdgeContainer edgeContainer) throws Exception {
        return ecFilter.filter(edgeContainer);
      }
    };
  }
}
