package edu.leipzig.grafs.operators.transform;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.interfaces.GraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Represents an Edge Transformation Operator. The given transformation function is applied to all
 * edges in the stream.
 */
public class EdgeTransformation implements GraphToGraphOperatorI,
    GraphCollectionToGraphCollectionOperatorI {

  /**
   * Function that is applied to the stream
   */
  protected MapFunction<Triplet, Triplet> tripletMapper;

  /**
   * Initializes this operator with the given transformation function.
   *
   * @param mapper transformation function that is used on every edge of the stream
   */
  public EdgeTransformation(final MapFunction<Element, Element> mapper) {
    this.tripletMapper = triplet -> {
      var transformedEdge = (Edge) mapper.map(triplet.getEdge());
      return new Triplet(transformedEdge, triplet.getSourceVertex(), triplet.getTargetVertex());
    };
  }

  /**
   * Applies this operator on the stream and returns the stream with the operator applied.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the edge transformation operator applied
   */
  @Override
  public DataStream<Triplet> execute(DataStream<Triplet> stream) {
    return stream.map(tripletMapper).name("Edge Transformation");
  }
}
