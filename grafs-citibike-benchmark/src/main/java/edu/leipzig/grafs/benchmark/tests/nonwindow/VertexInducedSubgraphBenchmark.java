package edu.leipzig.grafs.benchmark.tests.nonwindow;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.model.streaming.StreamI;
import java.io.Writer;
import java.util.concurrent.ExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VertexInducedSubgraphBenchmark extends AbstractBenchmark {

  public VertexInducedSubgraphBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new VertexInducedSubgraphBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream applyOperator(GraphStream stream) {
    return stream
        .vertexInducedSubgraph(v -> !v.getPropertyValue("name").getString().startsWith("P"));
  }

}
