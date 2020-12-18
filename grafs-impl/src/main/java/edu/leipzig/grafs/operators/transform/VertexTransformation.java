package edu.leipzig.grafs.operators.transform;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Represents a Vertex Transformation Operator. The given transformation function is applied to all vertices in the stream.
 */
public class VertexTransformation implements GraphToGraphOperatorI {

  /**
   * Function that is applied to the stream
   */
  protected MapFunction<EdgeContainer, EdgeContainer> ecMapper;

  /**
   * Initializes this operator with the given transformation function.
   * @param mapper transformation function that is used on every vertice of the stream
   */
  public VertexTransformation(final MapFunction<Vertex, Vertex> mapper) {
    this.ecMapper = ec -> {
      Vertex source = mapper.map(ec.getSourceVertex());
      Vertex target = mapper.map(ec.getTargetVertex());
      return new EdgeContainer(ec.getEdge(), source, target);
    };
  }

  /**
   * Applies this operator on the stream and returns the stream with the operator applied.
   * @param stream stream on which the operator should be applied
   * @return the stream with the vertex transformation operator applied
   */
  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return stream.map(ecMapper);
  }
}
