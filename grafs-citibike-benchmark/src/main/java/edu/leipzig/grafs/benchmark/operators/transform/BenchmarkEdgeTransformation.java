package edu.leipzig.grafs.benchmark.operators.transform;

import edu.leipzig.grafs.benchmark.operators.functions.MapFunctionWithMeter;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.transform.EdgeTransformation;
import java.io.IOException;
import java.io.Serializable;
import org.apache.flink.api.common.functions.MapFunction;

public class BenchmarkEdgeTransformation extends EdgeTransformation implements Serializable {

  protected BenchmarkEdgeTransformation() {
    super(null);
  }

  public BenchmarkEdgeTransformation(MapFunction<Edge, Edge> mapper) {
    this(mapper, "edgeTransformationMeter");
  }

  public BenchmarkEdgeTransformation(MapFunction<Edge, Edge> mapper, String meterName) {
    super(mapper);
    var oldMapper = tripletMapper;
    this.tripletMapper = new MapFunctionWithMeter<>(meterName) {
      @Override
      protected Triplet plainMap(Triplet ec) throws Exception {
        return oldMapper.map(ec);
      }
    };
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeObject(tripletMapper);
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    this.tripletMapper = (MapFunction<Triplet, Triplet>) in.readObject();
  }


}
