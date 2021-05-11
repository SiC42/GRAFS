package edu.leipzig.grafs.benchmark.tests.fixed;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.matching.DualSimulation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DualSimulationMatchingBenchmark extends AbstractFixedSizeBenchmark {

  public DualSimulationMatchingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new DualSimulationMatchingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    var query = "MATCH (v1)-[]->(v2)-[]->(v1)";
    return stream
        .window(TumblingEventTimeWindows.of(Time.minutes(2)))
        .callForGC(new DualSimulation(query));
  }

  // Has to be overriden if we change the query to filter specific vertices
  @Override
  protected boolean filter(Triplet<Vertex, Edge> triplet) {
    return super.filter(triplet);
  }
}
