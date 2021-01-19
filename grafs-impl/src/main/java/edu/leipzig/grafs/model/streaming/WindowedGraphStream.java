package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class WindowedGraphStream<W extends Window> extends
    AbstractWindowedStream<W,WindowedGraphStream<W>> implements
    WindowedGraphStreamOperators {

  public WindowedGraphStream(DataStream<Triplet> gcStream, FlinkConfig config,
      WindowAssigner<? super Triplet, W> window) {
    super(gcStream, config, window);
  }

  @Override
  protected WindowedGraphStream<W> getThis() {
    return this;
  }

  @Override
  public GraphStream callForGraph(WindowedGraphToGraphOperatorI operator) {
    var result = operator.execute(gcStream, wi);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(WindowedGraphToGraphCollectionOperatorI operator) {
    var result = operator.execute(gcStream, wi);
    return new GCStream(result, config);
  }
}
