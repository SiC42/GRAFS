package edu.leipzig.grafs.benchmarking.matching;

import edu.leipzig.grafs.benchmarking.functions.SimpleMeter;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.matching.Isomorphism;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BenchmarkIsomorphism<W extends Window> extends Isomorphism<W> {

  private final String meterName;

  public BenchmarkIsomorphism(String query, WindowAssigner<Object, W> window) {
    this(query, window, "isomorphismMeter");
  }

  public BenchmarkIsomorphism(String query, WindowAssigner<Object, W> window, String meterName) {
    super(query, window);
    this.meterName = meterName;
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return super.execute(stream).map(new SimpleMeter<>(meterName));
  }
}
