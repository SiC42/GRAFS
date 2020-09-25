package edu.leipzig.grafs.benchmarking.union;

import edu.leipzig.grafs.benchmarking.functions.SimpleMeter;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.union.UnionWithDuplicateInWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BenchmarkUnionWithDuplicateInWindow<W extends Window> extends
    UnionWithDuplicateInWindow<W> {


  private final String meterName;

  public BenchmarkUnionWithDuplicateInWindow(WindowAssigner<Object, W> window,
      EdgeStream... streams) {
    this(window, "unionWithDuplicateInWindowMeter", streams);
  }

  public BenchmarkUnionWithDuplicateInWindow(WindowAssigner<Object, W> window, String meterName,
      EdgeStream... streams) {
    super(window, streams);
    this.meterName = meterName;
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return super.execute(stream).map(new SimpleMeter<>(meterName));
  }
}
