package streaming.operators.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.operators.OperatorI;

public class EdgeTransformation implements OperatorI {

  private final MapFunction<Edge, Edge> mapper;

  public EdgeTransformation(final MapFunction<Edge, Edge> mapper) {
    this.mapper = mapper;
  }


  @Override
  public DataStream<Edge> execute(DataStream<Edge> stream) {
    return transform(stream);
  }

  private DataStream<Edge> transform(DataStream<Edge> stream) {
    DataStream<Edge> filteredStream = stream.map(mapper);
    return filteredStream;
  }
}
