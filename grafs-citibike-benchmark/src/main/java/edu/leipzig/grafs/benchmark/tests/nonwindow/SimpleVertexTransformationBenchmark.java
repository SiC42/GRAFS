package edu.leipzig.grafs.benchmark.tests.nonwindow;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.EdgeStream;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class SimpleVertexTransformationBenchmark extends AbstractBenchmark {

  public SimpleVertexTransformationBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new SimpleVertexTransformationBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator() {
    return this.edgeStream.transformEdges(e -> {
      e.setProperty("edgetransformed", PropertyValue.create(true));
      return e;
    });
  }

}
