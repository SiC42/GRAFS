package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowsI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class GCStream extends AbstractStream<GCStream> implements GCStreamOperators {

  /**
   * Constructs a graph stream with the given data stream and config.
   *
   * @param stream data stream that holds <tt>Triplet</tt>
   * @param config config used for the stream
   */
  public GCStream(
      DataStream<Triplet> stream, FlinkConfig config) {
    super(stream, config);
  }

  /**
   * Constructs an graph collection stream using the given kafka consumer and stream config.
   *
   * @param fkConsumer kafka consumer from which the information are fetched
   * @param config     config used for the stream
   * @return graph collection stream that is extracted from source
   */
  public static GraphStream fromSource(FlinkKafkaConsumer<Triplet> fkConsumer,
      FlinkConfig config, int parallelism) {
    DataStreamSource<Triplet> stream;
    if (parallelism > 0) {
      stream = config.getExecutionEnvironment().addSource(fkConsumer).setParallelism(parallelism);
    } else {
      stream = config.getExecutionEnvironment().addSource(fkConsumer);
    }

    return new GraphStream(stream, config);
  }

  @Override
  protected GCStream getThis() {
    return this;
  }

  @Override
  public GraphStream callForGraph(GraphCollectionToGraphOperatorI operator) {
    DataStream<Triplet> result = operator.execute(stream);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(GraphCollectionToGraphCollectionOperatorI operator) {
    var result = operator.execute(stream);
    return new GCStream(result, config);
  }

  public <FW extends Window, W extends WindowsI<FW>> InitialWindowBuilder<GraphStream, W> callForGraph(
      WindowedGraphCollectionToGraphOperatorI<W> operator) {
    return new InitialWindowBuilder<>(new GraphStream(stream, config), operator);
  }

  //@Override
  public <FW extends Window, W extends WindowsI<? extends FW>> InitialWindowBuilder<GCStream, W> callForGC(
      WindowedGraphCollectionToGraphCollectionOperatorI<W> operator) {
    return new InitialWindowBuilder<>(new GCStream(stream, config), operator);
  }

}
