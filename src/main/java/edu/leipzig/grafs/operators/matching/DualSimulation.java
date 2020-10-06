package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.matching.logic.DualSimulationProcess;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class DualSimulation<W extends Window> extends AbstractMatchingOperator<W> {

  public DualSimulation(String query, WindowAssigner<Object, W> window) {
    this(query, window, null);
  }

  public DualSimulation(String query, WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    super(query, window, trigger);
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    var preProcessedStream = preProcessAndApplyWindow(stream);
    return preProcessedStream.process(new DualSimulationProcess<>(queryGraph));
  }
}
