package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.matching.Isomorphism;

public class IsomorphismMatchingBenchmark extends AbstractWindowBenchmark {

  public IsomorphismMatchingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new IsomorphismMatchingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperatorWithWindow(GraphStream stream) {
    var query = "(v1)-[]->(v2)-[]->(v1)";
    return stream.callForGC(new Isomorphism(query)).withWindow(window).apply();
  }

}
