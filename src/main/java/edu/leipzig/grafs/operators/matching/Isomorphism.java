package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.matching.logic.IsomorphismMatchingProcess;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class Isomorphism<W extends Window> extends AbstractMatchingOperator<W> {

  public Isomorphism(String query, WindowAssigner<Object, W> window) {
    this(query, window, null);
  }

  public Isomorphism(String query, WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    super(query, window, trigger);
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    var preProcessedStream = preProcessAndApplyWindow(stream);
    return preProcessedStream.process(new IsomorphismMatchingProcess<>(queryGraph));
  }
}
