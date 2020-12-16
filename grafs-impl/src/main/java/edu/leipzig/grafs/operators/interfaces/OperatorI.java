package edu.leipzig.grafs.operators.interfaces;

import edu.leipzig.grafs.model.EdgeContainer;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Interface for all operators which should be applied to an edge stream.
 */
public interface OperatorI {

  /**
   * Applies the given operator to the stream.
   * @param stream stream on which the operator should be applied
   * @return the stream with the applied operator
   */
  DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream);

}
