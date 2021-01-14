package edu.leipzig.grafs.benchmark.operators.union;

import edu.leipzig.grafs.benchmark.operators.functions.SimpleMeter;
import edu.leipzig.grafs.model.Triplet;
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
  public DataStream<Triplet> execute(DataStream<Triplet> stream) {
    return super.execute(stream).map(new SimpleMeter<>(meterName));
  }
}
