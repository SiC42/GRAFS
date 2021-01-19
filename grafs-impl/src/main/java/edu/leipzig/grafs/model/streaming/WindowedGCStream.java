package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class WindowedGCStream<W extends Window> extends
    AbstractWindowedStream<W,WindowedGCStream<W>> implements
    WindowedGCOperators {

  public WindowedGCStream(DataStream<Triplet> gcStream, FlinkConfig config,
      WindowAssigner<? super Triplet, W> window) {
    super(gcStream, config, window);
  }

  @Override
  protected WindowedGCStream<W> getThis() {
    return this;
  }

  @Override
  public GraphStream callForGraph(WindowedGraphCollectionToGraphOperatorI operator) {
    var result = operator.execute(gcStream, wi);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(WindowedGraphCollectionToGraphCollectionOperatorI operator) {
    var result = operator.execute(gcStream, wi);
    return new GCStream(result, config);
  }
}
