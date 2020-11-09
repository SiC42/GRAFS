package edu.leipzig.grafs.benchmark.tests.nonwindow;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.EdgeStream;

public class NoOpBenchmark extends AbstractBenchmark {

  public NoOpBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new NoOpBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator(EdgeStream edgeStream) {
    return edgeStream;
  }

}
