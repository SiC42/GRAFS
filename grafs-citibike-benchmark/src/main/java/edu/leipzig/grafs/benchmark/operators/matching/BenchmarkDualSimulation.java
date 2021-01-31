package edu.leipzig.grafs.benchmark.operators.matching;

import edu.leipzig.grafs.benchmark.operators.functions.SimpleMeter;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.operators.matching.DualSimulation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BenchmarkDualSimulation extends DualSimulation {


  private final String meterName;

  public BenchmarkDualSimulation(String query) {
    this(query, "dualSimulationMeter");
  }

  public BenchmarkDualSimulation(String query, String meterName) {
    super(query);

    this.meterName = meterName;
  }

  @Override
  public <W extends Window> DataStream<Triplet> execute(DataStream<Triplet> stream,
      WindowingInformation<W> wi) {
    return super.execute(stream, wi).map(new SimpleMeter<>(meterName));
  }
}
