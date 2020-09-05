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
      var transformedEdge = mapper.map(ec.getEdge());
      return new EdgeContainer(transformedEdge, ec.getSourceVertex(), ec.getTargetVertex());
    };
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return stream.map(ecMapper);
  }
}
