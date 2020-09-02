package edu.leipzig.grafs.operators.transform;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class EdgeTransformation implements GraphToGraphOperatorI {

  private final MapFunction<EdgeContainer, EdgeContainer> ecMapper;

  public EdgeTransformation(final MapFunction<Edge, Edge> mapper) {
    this.ecMapper = ec -> {
      mapper.map(ec.getEdge());
      return ec;
    };
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return stream.map(ecMapper);
  }
}
