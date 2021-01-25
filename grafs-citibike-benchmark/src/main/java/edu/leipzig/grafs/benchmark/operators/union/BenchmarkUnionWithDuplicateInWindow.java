package edu.leipzig.grafs.benchmark.operators.union;

import edu.leipzig.grafs.benchmark.operators.functions.SimpleMeter;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.nonwindow.GraphStream;
import edu.leipzig.grafs.model.streaming.window.AbstractWindowedStream.WindowInformation;
import edu.leipzig.grafs.operators.union.UnionWithDuplicateInWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BenchmarkUnionWithDuplicateInWindow extends UnionWithDuplicateInWindow {


  private final String meterName;

  public BenchmarkUnionWithDuplicateInWindow(GraphStream... streams) {
    this("unionWithDuplicateInWindowMeter", streams);
  }

  public BenchmarkUnionWithDuplicateInWindow(String meterName,
      GraphStream... streams) {
    super(streams);
    this.meterName = meterName;
  }

  @Override
  public <W extends Window> DataStream<Triplet> execute(DataStream<Triplet> stream,
      WindowInformation<W> wi) {
    return super.execute(stream, wi).map(new SimpleMeter<>(meterName));
  }
}
