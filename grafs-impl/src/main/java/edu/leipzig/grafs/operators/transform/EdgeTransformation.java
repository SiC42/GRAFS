package edu.leipzig.grafs.operators.transform;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Represents an Edge Transformation Operator. The given transformation function is applied to all edges in the stream.
 */
public class EdgeTransformation implements GraphToGraphOperatorI {

  /**
   * Function that is applied to the stream
   */
  protected MapFunction<EdgeContainer, EdgeContainer> ecMapper;

  /**
   * Initializes this operator with the given transformation function.
   * @param mapper transformation function that is used on every edge of the stream
   */
  public EdgeTransformation(final MapFunction<Edge, Edge> mapper) {
    this.ecMapper = ec -> {
      var transformedEdge = mapper.map(ec.getEdge());
      return new EdgeContainer(transformedEdge, ec.getSourceVertex(), ec.getTargetVertex());
    };
  }

  /**
   * Applies this operator on the stream and returns the stream with the operator applied.
   * @param stream stream on which the operator should be applied
   * @return the stream with the edge transformation operator applied
   */
  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return stream.map(ecMapper);
  }
}
