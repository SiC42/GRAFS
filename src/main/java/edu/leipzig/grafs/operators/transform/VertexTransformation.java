package edu.leipzig.grafs.operators.transform;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class VertexTransformation implements GraphToGraphOperatorI {

  protected MapFunction<EdgeContainer, EdgeContainer> ecMapper;

  protected VertexTransformation() {
  }

  public VertexTransformation(final MapFunction<Vertex, Vertex> mapper) {
    this.ecMapper = ec -> {
      Vertex source = mapper.map(ec.getSourceVertex());
      Vertex target = mapper.map(ec.getTargetVertex());
      return new EdgeContainer(ec.getEdge(), source, target);
    };
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return stream.map(ecMapper);
  }
}
