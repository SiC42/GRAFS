package edu.leipzig.grafs.operators.interfaces;

import edu.leipzig.grafs.model.Triplet;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Interface for all operators which should be applied to an triplet stream.
 */
public interface OperatorI {

  /**
   * Applies the given operator to the stream.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the applied operator
   */
  DataStream<Triplet> execute(DataStream<Triplet> stream);

}
