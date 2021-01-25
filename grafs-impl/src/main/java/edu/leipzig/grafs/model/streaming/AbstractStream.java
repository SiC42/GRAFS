package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;

public abstract class AbstractStream {


  protected final DataStream<Triplet> stream;
  protected final FlinkConfig config;

  /**
   * Constructs an triplet stream with the given data stream and config.
   *
   * @param stream data stream that holds <tt>Triplet</tt>
   * @param config     config used for the stream
   */
  public AbstractStream(DataStream<Triplet> stream, FlinkConfig config) {
    this.stream = stream.assignTimestampsAndWatermarks(config.getWatermarkStrategy());
    this.config = config;
  }

  /**
   * Returns the underlying data stream.
   *
   * @return the underlying data stream
   */
  public DataStream<Triplet> getDataStream() {
    return stream;
  }

}
