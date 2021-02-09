package edu.leipzig.grafs.operators.interfaces.nonwindow;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Interface for all operators which should be applied to an triplet stream.
 */
public interface NonWindowedOperatorI {

  /**
   * Applies the given operator to the stream.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the applied operator
   */
  DataStream<Triplet<Vertex, Edge>> execute(DataStream<Triplet<Vertex, Edge>> stream);

}
