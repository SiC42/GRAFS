package edu.leipzig.grafs.model.streaming.window;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.nonwindow.GCStream;
import edu.leipzig.grafs.model.streaming.nonwindow.GraphStream;
import edu.leipzig.grafs.operators.interfaces.window.WindowGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowGraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class WindowedGCStream<W extends Window> extends
    AbstractWindowedStream<W, WindowedGCStream<W>> implements
    WindowedGCStreamOperators {

  public WindowedGCStream(DataStream<Triplet> stream, FlinkConfig config,
      WindowAssigner<? super Triplet, W> window) {
    super(stream, config, window);
  }

  @Override
  protected WindowedGCStream<W> getThis() {
    return this;
  }

  @Override
  public GraphStream callForGraph(WindowGraphCollectionToGraphOperatorI operator) {
    var result = operator.execute(stream, wi);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(WindowGraphCollectionToGraphCollectionOperatorI operator) {
    var result = operator.execute(stream, wi);
    return new GCStream(result, config);
  }
}
