package edu.leipzig.grafs.benchmarking.transform;

import edu.leipzig.grafs.benchmarking.functions.MapFunctionWithMeter;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.transform.VertexTransformation;
import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.functions.MapFunction;

public class BenchmarkVertexTransformation extends VertexTransformation implements Serializable {


  protected BenchmarkVertexTransformation() {
    super(null);
  }

  public BenchmarkVertexTransformation(MapFunction<Vertex, Vertex> mapper) {
    this(mapper, "vertexTransformationMeter");
  }

  public BenchmarkVertexTransformation(MapFunction<Vertex, Vertex> mapper, String meterName) {
    super(mapper);
    var oldMapper = ecMapper;
    this.ecMapper = new MapFunctionWithMeter<>(meterName) {
      @Override
      protected EdgeContainer plainMap(EdgeContainer ec) throws Exception {
        return oldMapper.map(ec);
      }
    };
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeObject(ecMapper);
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    this.ecMapper = (MapFunction<EdgeContainer, EdgeContainer>) in.readObject();

  }

}
