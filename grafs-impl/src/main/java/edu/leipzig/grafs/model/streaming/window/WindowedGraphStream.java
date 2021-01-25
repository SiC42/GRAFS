package edu.leipzig.grafs.model.streaming.window;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.nonwindow.GCStream;
import edu.leipzig.grafs.model.streaming.nonwindow.GraphStream;
import edu.leipzig.grafs.operators.interfaces.window.WindowGraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowGraphToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class WindowedGraphStream<W extends Window> extends
    AbstractWindowedStream<W, WindowedGraphStream<W>> implements
    WindowedGraphStreamOperators {

  public WindowedGraphStream(DataStream<Triplet> stream, FlinkConfig config,
      WindowAssigner<? super Triplet, W> window) {
    super(stream, config, window);
  }

  @Override
  protected WindowedGraphStream<W> getThis() {
    return this;
  }

  @Override
  public GraphStream callForGraph(WindowGraphToGraphOperatorI operator) {
    var result = operator.execute(stream, wi);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(WindowGraphToGraphCollectionOperatorI operator) {
    var result = operator.execute(stream, wi);
    return new GCStream(result, config);
  }
}
