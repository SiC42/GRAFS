package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.EdgeStream;

public class DualSimulationMatchingBenchmark extends AbstractWindowBenchmark {

  public DualSimulationMatchingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    AbstractBenchmark benchmark = new DualSimulationMatchingBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator(EdgeStream edgeStream) {
    var query = "(v1)-[]->(v2)-[]->(v1)";
    if (useTrigger) {
      return edgeStream.dualSimulation(query, window, countTrigger);
    } else {
      return edgeStream.dualSimulation(query, window);
    }
  }

}
