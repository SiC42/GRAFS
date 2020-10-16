package edu.leipzig.grafs.benchmark.tests.nonwindow;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.EdgeStream;

public class SimpleVertexInducedSubgraphBenchmark extends AbstractBenchmark {

  public SimpleVertexInducedSubgraphBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new SimpleVertexInducedSubgraphBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator() {
    return this.edgeStream.vertexInducedSubgraph(v -> v.hasProperty("key"));
  }

}
