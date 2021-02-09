package edu.leipzig.grafs.benchmark.tests.fixed;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;


public class NoOpBenchmark extends AbstractFixedSizeBenchmark {

  public NoOpBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new NoOpBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    return stream;
  }

}
