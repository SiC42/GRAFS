package streaming.operators.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeStream;
import streaming.model.Vertex;
import streaming.operators.OperatorI;

public class VertexTransformation implements OperatorI {

  private final MapFunction<Vertex, Vertex> mapper;

  public VertexTransformation(
      MapFunction<Vertex, Vertex> mapper) {
    this.mapper = mapper;
  }


  @Override
  public EdgeStream execute(DataStream<Edge> stream) {
    return transform(stream);
  }

  public EdgeStream transform(DataStream<Edge> stream) {
    MapFunction<Edge, Edge> transformVerticesFunction =
        edge -> {
          Vertex from = mapper.map(edge.getSource());
          Vertex to = mapper.map(edge.getTarget());
          return new Edge(from, to, edge.getGei());
        };
    EdgeTransformation eT = new EdgeTransformation(transformVerticesFunction);
    return eT.execute(stream);
  }
}
