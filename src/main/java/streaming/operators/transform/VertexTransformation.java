package streaming.operators.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Vertex;
import streaming.operators.OperatorI;

public class VertexTransformation implements OperatorI {

  private final MapFunction<Vertex, Vertex> mapper;

  public VertexTransformation(final MapFunction<Vertex, Vertex> mapper) {
    this.mapper = mapper;
  }


  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return transform(stream);
  }

  public DataStream<EdgeContainer> transform(DataStream<EdgeContainer> stream) {
    MapFunction<EdgeContainer, EdgeContainer> transformVerticesFunction =
        ec -> {
          Vertex target = mapper.map(ec.getTargetVertex());
          Vertex source = mapper.map(ec.getSourceVertex());
          return new EdgeContainer(ec.getEdge(), target, source);
        };
    return stream.map(transformVerticesFunction);
  }
}
