package edu.leipzig.grafs.benchmark.tests.nonwindow;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.EdgeStream;

public class SimpleEdgeInducedSubgraphBenchmark extends AbstractBenchmark {

  public SimpleEdgeInducedSubgraphBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new SimpleEdgeInducedSubgraphBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator(EdgeStream edgeStream) {
    return edgeStream.edgeInducedSubgraph(e -> Math.random() >= 0.5);
  }

}
