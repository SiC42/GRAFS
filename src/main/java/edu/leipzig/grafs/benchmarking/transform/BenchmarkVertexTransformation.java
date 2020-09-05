package edu.leipzig.grafs.benchmarking.transform;

import edu.leipzig.grafs.benchmarking.functions.MapFunctionWithMeter;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.transform.VertexTransformation;
import org.apache.flink.api.common.functions.MapFunction;

public class BenchmarkVertexTransformation extends VertexTransformation {

  public BenchmarkVertexTransformation(MapFunction<Vertex, Vertex> mapper) {
    this(mapper, "vertexTransformationMeter");
  }

  public BenchmarkVertexTransformation(MapFunction<Vertex, Vertex> mapper, String meterName) {
    super(mapper);
    this.ecMapper = new MapFunctionWithMeter<>(meterName) {
      @Override
      public EdgeContainer map(EdgeContainer ec) throws Exception {
        return ecMapper.map(ec);
      }
    };
  }

}
