package edu.leipzig.grafs.benchmark.tests.nonwindow;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeStream;

public class SimpleEdgeInducedSubgraphBenchmark extends AbstractBenchmark {

  public SimpleEdgeInducedSubgraphBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new SimpleEdgeInducedSubgraphBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator() {
    return this.edgeStream.edgeInducedSubgraph(Edge::isReverse);
  }

}
