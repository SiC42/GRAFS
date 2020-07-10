package edu.leipzig.grafs.operators.transform;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.OperatorI;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class VertexTransformation implements OperatorI {

  private final MapFunction<EdgeContainer, EdgeContainer> ecMapper;

  public VertexTransformation(final MapFunction<Vertex, Vertex> mapper) {
    this.ecMapper = ec -> {
      Vertex target = mapper.map(ec.getTargetVertex());
      Vertex source = mapper.map(ec.getSourceVertex());
      return new EdgeContainer(ec.getEdge(), target, source);
    };
    ;
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return stream.map(ecMapper);
  }
}
