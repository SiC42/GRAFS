package edu.leipzig.grafs.model.streaming.nonwindow;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.window.WindowedGraphStream;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Model that abstracts the data stream to a edge(container)-stream.
 */
public class GraphStream extends AbstractNonWindowedStream implements GraphStreamOperators {

  /**
   * Constructs an graph stream with the given data stream and config.
   *
   * @param stream data stream that holds <tt>Triplet</tt>
   * @param config config used for the stream
   */
  public GraphStream(DataStream<Triplet> stream, FlinkConfig config) {
    super(stream, config);
  }

  /**
   * Constructs an graph stream using the given kafka consumer and stream config.
   *
   * @param fkConsumer kafka consumer from which the information are fetched
   * @param config     config used for the stream
   * @return graph stream that is extracted from source
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

  /**
   * Creates an triplet stream from this stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  public GraphStream callForGraph(GraphToGraphOperatorI operator) {
    DataStream<Triplet> result = operator.execute(stream);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(GraphToGraphCollectionOperatorI operator) {
    DataStream<Triplet> result = operator.execute(stream);
    return new GCStream(result, config);
  }

  public <W extends Window> WindowedGraphStream<W> window(
      WindowAssigner<? super Triplet, W> window) {
    return new WindowedGraphStream<>(stream, config, window);
  }
}