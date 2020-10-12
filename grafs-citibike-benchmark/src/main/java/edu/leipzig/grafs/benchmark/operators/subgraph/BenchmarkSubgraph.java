package edu.leipzig.grafs.benchmark.operators.subgraph;

import edu.leipzig.grafs.benchmark.operators.functions.FilterFunctionWithMeter;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.subgraph.Subgraph;
import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.functions.FilterFunction;

public class BenchmarkSubgraph extends Subgraph implements Serializable {

  protected BenchmarkSubgraph() {
    super();
  }

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
    var tempFilter = ecFilter;
    ecFilter = new FilterFunctionWithMeter<>(meterName) {
      @Override
      public boolean plainFilter(EdgeContainer edgeContainer) throws Exception {
        return tempFilter.filter(edgeContainer);
      }
    };
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeObject(ecFilter);
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    this.ecFilter = (FilterFunction<EdgeContainer>) in.readObject();
  }
}
