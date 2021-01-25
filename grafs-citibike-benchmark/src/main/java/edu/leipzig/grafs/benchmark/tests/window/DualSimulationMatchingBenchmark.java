package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.streaming.nonwindow.AbstractNonWindowedStream;
import edu.leipzig.grafs.model.streaming.window.WindowedGraphStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class DualSimulationMatchingBenchmark extends AbstractWindowBenchmark {

  public DualSimulationMatchingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    AbstractBenchmark benchmark = new DualSimulationMatchingBenchmark(args);
    benchmark.execute();
  }

  public <W extends Window> AbstractNonWindowedStream applyOperator(WindowedGraphStream<W> stream) {
    var query = "(v1)-[]->(v2)-[]->(v1)";
    return stream.dualSimulation(query);
  }

}
