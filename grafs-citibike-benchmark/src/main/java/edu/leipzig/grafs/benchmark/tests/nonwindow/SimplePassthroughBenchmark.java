package edu.leipzig.grafs.benchmark.tests.nonwindow;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.EdgeStream;

public class SimplePassthroughBenchmark extends AbstractBenchmark {


  public SimplePassthroughBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new SimplePassthroughBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator(EdgeStream edgeStream) {
    return edgeStream;
  }

}
