package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.model.EdgeStream;

public class SimpleIsomorphismMatchingBenchmark extends AbstractWindowBenchmark {

  public SimpleIsomorphismMatchingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new SimpleIsomorphismMatchingBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator() {
    var query = "(v1)-[]->(v2)-[]->(v1)";
    if (useTrigger) {
      return this.edgeStream.isomorphismMatching(query, window, countTrigger);
    } else {
      return this.edgeStream.isomorphismMatching(query, window);
    }
  }

}
