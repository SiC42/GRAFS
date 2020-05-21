package streaming.operators.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeStream;
import streaming.operators.OperatorI;

public class EdgeTransformation implements OperatorI {

  private final MapFunction<Edge, Edge> mapper;

  public EdgeTransformation(
      MapFunction<Edge, Edge> mapper) {
    this.mapper = mapper;
  }


  @Override
  public EdgeStream execute(DataStream<Edge> stream) {
    return transform(stream);
  }

  private EdgeStream transform(DataStream<Edge> stream) {
    DataStream<Edge> filteredStream = stream.map(mapper);
    return new EdgeStream(filteredStream);
  }
}
