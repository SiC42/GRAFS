package edu.leipzig.grafs.benchmark.tests.nonwindow;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;

public class EdgeInducedSubgraphBenchmark extends AbstractBenchmark {

  public EdgeInducedSubgraphBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new EdgeInducedSubgraphBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    return stream.edgeInducedSubgraph(e -> e.getPropertyValue("gender").getString().equals("1"));
  }

}
