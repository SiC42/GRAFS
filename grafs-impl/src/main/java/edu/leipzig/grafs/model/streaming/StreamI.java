package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface StreamI {

  /**
   * Returns the underlying data stream.
   *
   * @return the underlying data stream
   */
  DataStream<Triplet> getDataStream();

}
