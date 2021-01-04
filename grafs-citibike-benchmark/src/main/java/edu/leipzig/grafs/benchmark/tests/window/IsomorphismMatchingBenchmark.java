package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.model.EdgeStream;

public class IsomorphismMatchingBenchmark extends AbstractWindowBenchmark {

  public IsomorphismMatchingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new IsomorphismMatchingBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator(EdgeStream edgeStream) {
    var query = "(v1)-[]->(v2)-[]->(v1)";
    if (useTrigger) {
      return edgeStream.isomorphismMatching(query, window, countTrigger);
    } else {
      return edgeStream.isomorphismMatching(query, window);
    }
  }

}
