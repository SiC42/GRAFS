package edu.leipzig.grafs.benchmark.tests.streaming.latency;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;

public class VertexInducedSubgraphBenchmark extends AbstractLatencyBenchmark {

  public VertexInducedSubgraphBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new VertexInducedSubgraphBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    return stream
        .vertexInducedSubgraph(v -> !v.hasProperty("name"));
  }

}
