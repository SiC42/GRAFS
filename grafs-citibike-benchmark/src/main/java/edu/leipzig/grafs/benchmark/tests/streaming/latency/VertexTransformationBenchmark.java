package edu.leipzig.grafs.benchmark.tests.streaming.latency;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class VertexTransformationBenchmark extends AbstractLatencyBenchmark {

  public VertexTransformationBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new VertexTransformationBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    return stream.transformVertices(v -> {
      v.setProperty("vertexTransformed", PropertyValue.create(true));
      return v;
    });
  }

}
