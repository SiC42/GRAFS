package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.EdgeStream;

public class SimpleDualSimulationMatchingBenchmark extends AbstractWindowBenchmark {

  public SimpleDualSimulationMatchingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    AbstractBenchmark benchmark = new SimpleDualSimulationMatchingBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator() {
    var query = "(v1)-[]->(v2)-[]->(v1)";
    if (useTrigger) {
      return this.edgeStream.dualSimulation(query, window, countTrigger);
    } else {
      return this.edgeStream.dualSimulation(query, window);
    }
  }

}
