package edu.leipzig.grafs.model.streaming.nonwindow;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.util.FlinkConfig;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public abstract class AbstractNonWindowStream extends AbstractStream {

  /**
   * Constructs an triplet stream with the given data stream and config.
   *
   * @param stream data stream that holds <tt>Triplet</tt>
   * @param config     config used for the stream
   */
  public AbstractNonWindowStream(DataStream<Triplet> stream, FlinkConfig config) {
    super(stream, config);
  }

  /**
   * Adds a sink function to the stream, which determines what should happen with the stream at the
   * end.
   * <p>
   * Only works once!
   *
   * @param sinkFunction The object containing the sink's invoke function.
   */
  public void addSink(SinkFunction<Triplet> sinkFunction) {
    stream.addSink(sinkFunction);
  }

  /**
   * Prints the stream to stdout.
   */
  public void print() {
    stream.print();
  }

  /**
   * Collects the stream into a iterator
   *
   * @return iterator of the stream content
   * @throws IOException
   */
  public Iterator<Triplet> collect() throws IOException {
    return DataStreamUtils.collect(stream);
  }

}
