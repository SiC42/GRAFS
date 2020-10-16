package edu.leipzig.grafs.model;

import edu.leipzig.grafs.operators.interfaces.OperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Model that abstracts the data stream to a edge(container)-stream.
 */
public class EdgeStream implements EdgeStreamOperators {

  private final DataStream<EdgeContainer> edgeStream;
  private final FlinkConfig config;

  /**
   * Constructs an edge stream with the given data stream and config.
   *
   * @param edgeStream data stream that holds <tt>EdgeContainer</tt>
   * @param config     config used for the stream
   */
  public EdgeStream(DataStream<EdgeContainer> edgeStream, FlinkConfig config) {
    this.edgeStream = edgeStream.assignTimestampsAndWatermarks(config.getWatermarkStrategy());
    this.config = config;
  }

  /**
   * Constructs an edge stream using the given kafka consumer and stream config.
   *
   * @param fkConsumer kafka consumer from which the information are fetched
   * @param config     config used for the stream
   * @return
   */
  public static EdgeStream fromSource(FlinkKafkaConsumer<EdgeContainer> fkConsumer,
      FlinkConfig config) {
    var stream = config.getExecutionEnvironment().addSource(fkConsumer);
    return new EdgeStream(stream, config);
  }

  /**
   * Adds a sink function to the stream, which determines what should happen with the stream at the end.
   *
   * Only works once!
   *
   * @param sinkFunction The object containing the sink's invoke function.
   */
  public void addSink(SinkFunction<EdgeContainer> sinkFunction){
    edgeStream.addSink(sinkFunction);
  }

  /**
   * Creates an edge stream from this stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  public EdgeStream callForStream(OperatorI operator) {
    DataStream<EdgeContainer> result = operator.execute(edgeStream);
    return new EdgeStream(result, config);
  }

  /**
   * Returns the underlying data stream.
   *
   * @return the underlying data stream
   */
  public DataStream<EdgeContainer> getDataStream() {
    return edgeStream;
  }

  /**
   * Prints the stream to stdout.
   */
  public void print() {
    edgeStream.print();
  }

  /**
   * Collects the stream into a iterator
   *
   * @return iterator of the stream content
   * @throws IOException
   */
  public Iterator<EdgeContainer> collect() throws IOException {
    return DataStreamUtils.collect(edgeStream);
  }
}