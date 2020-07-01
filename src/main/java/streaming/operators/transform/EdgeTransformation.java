package streaming.operators.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.operators.OperatorI;

public class EdgeTransformation implements OperatorI {

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
