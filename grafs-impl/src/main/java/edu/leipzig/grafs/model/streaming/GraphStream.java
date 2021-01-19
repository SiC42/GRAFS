package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Model that abstracts the data stream to a edge(container)-stream.
 */
public class GraphStream extends AbstractStream implements GraphStreamOperators {

  /**
   * Constructs an triplet stream with the given data stream and config.
   *
   * @param edgeStream data stream that holds <tt>Triplet</tt>
   * @param config     config used for the stream
   */
  public GraphStream(DataStream<Triplet> edgeStream, FlinkConfig config) {
    super(edgeStream, config);
  }

  /**
   * Creates an triplet stream from this stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  public GraphStream callForGraph(GraphToGraphOperatorI operator) {
    DataStream<Triplet> result = operator.execute(edgeStream);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(GraphToGraphCollectionOperatorI operator) {
    DataStream<Triplet> result = operator.execute(edgeStream);
    return new GCStream(result, config);
  }

  public <W extends Window> WindowedGraphStream<W> window(WindowAssigner<? super Triplet, W> window) {
    return new WindowedGraphStream<>(edgeStream, config, window);
  }
}