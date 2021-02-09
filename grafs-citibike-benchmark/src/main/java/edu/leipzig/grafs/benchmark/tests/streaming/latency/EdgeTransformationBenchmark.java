package edu.leipzig.grafs.benchmark.tests.streaming.latency;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class EdgeTransformationBenchmark extends AbstractLatencyBenchmark {

  public EdgeTransformationBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new EdgeTransformationBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    return stream.transformEdges(e -> {
      e.setProperty("edgetransformed", PropertyValue.create(true));
      return e;
    });
  }

}
