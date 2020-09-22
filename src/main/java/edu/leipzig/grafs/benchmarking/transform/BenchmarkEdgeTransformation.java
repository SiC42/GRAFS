package edu.leipzig.grafs.benchmarking.transform;

import edu.leipzig.grafs.benchmarking.functions.MapFunctionWithMeter;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.transform.EdgeTransformation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.metrics.Meter;

public class BenchmarkEdgeTransformation extends EdgeTransformation {

  private transient Meter meter;

  public BenchmarkEdgeTransformation(MapFunction<Edge, Edge> mapper) {
    this(mapper, "edgeTransformationMeter");
  }

  public BenchmarkEdgeTransformation(MapFunction<Edge, Edge> mapper, String meterName) {
    super(mapper);
    this.ecMapper = new MapFunctionWithMeter<>(meterName) {
      @Override
      public EdgeContainer plainMap(EdgeContainer ec) throws Exception {
        return ecMapper.map(ec);
      }
    };
  }


}
