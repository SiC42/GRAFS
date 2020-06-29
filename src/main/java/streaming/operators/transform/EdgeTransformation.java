package streaming.operators.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.GraphElement;
import streaming.operators.OperatorI;

public class EdgeTransformation implements OperatorI {

  private final MapFunction<Edge, Edge> mapper;

  public EdgeTransformation(final MapFunction<Edge, Edge> mapper) {
    this.mapper = mapper;
  }


  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return transform(stream);
  }

  private DataStream<EdgeContainer> transform(DataStream<EdgeContainer> stream) {
    // TODO: Make this save (can there be deepcopy-refs here?)
    MapFunction<EdgeContainer, EdgeContainer> edgeContainerMapper = ec -> {
      mapper.map(ec.getEdge());
      return ec;
    };

    DataStream<EdgeContainer> filteredStream = stream.map(edgeContainerMapper);
    return filteredStream;
  }
}
