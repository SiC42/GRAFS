package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.matching.DualSimulation;

public class DualSimulationMatchingBenchmark extends AbstractWindowBenchmark {

  public DualSimulationMatchingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    AbstractBenchmark benchmark = new DualSimulationMatchingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperatorWithWindow(GraphStream stream) {
    var query = "(v1)-[]->(v2)-[]->(v1)";
    return stream.callForGC(new DualSimulation(query))
        .withWindow(window).apply();
  }

}
