package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.interfaces.GraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class GCStream extends AbstractStream implements GCStreamOperators {

  /**
   * Constructs an edge stream with the given data stream and config.
   *
   * @param edgeStream data stream that holds <tt>Triplet</tt>
   * @param config     config used for the stream
   */
  public GCStream(
      DataStream<Triplet> edgeStream, FlinkConfig config) {
    super(edgeStream, config);
  }

  @Override
  public GraphStream callForGraph(GraphCollectionToGraphOperatorI operator) {
    DataStream<Triplet> result = operator.execute(edgeStream);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(GraphCollectionToGraphCollectionOperatorI operator) {
    DataStream<Triplet> result = operator.execute(edgeStream);
    return new GCStream(result, config);
  }

  public <W extends Window> WindowedGCStream<W> window(WindowAssigner<? super Triplet, W> window) {
    return new WindowedGCStream<>(edgeStream, config, window);
  }
}
