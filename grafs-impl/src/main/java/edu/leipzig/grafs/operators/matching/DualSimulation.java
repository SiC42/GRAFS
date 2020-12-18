package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.matching.logic.DualSimulationProcess;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * This operator represents a Dual Simulation on a window of the stream and is based on the
 * algorithm in <a href="https://ieeexplore.ieee.org/abstract/document/6906821">"DualIso: An
 * Algorithm for Subgraph Pattern Matching on Very Large Labeled Graphs"</a> by Saltz et al.
 *
 * @param <W> type of window that is used in this operation
 */
public class DualSimulation<W extends Window> extends AbstractMatchingOperator<W> {

  /**
   * Initializes the operator with the given parameters (without trigger).
   * @param query query string that is used to make the query graph
   * @param window window that for this operation
   */
  public DualSimulation(String query, WindowAssigner<Object, W> window) {
    this(query, window, null);
  }

  /**
   * Initializes the operator with the given parameters.
   * @param query query string that is used to make the query graph
   * @param window window that for this operation
   * @param trigger optional window trigger that is used for this operation
   */
  public DualSimulation(String query, WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    super(query, window, trigger);
  }

  /**
   * Applies this operator on the stream and returns the stream with the operator applied.
   * @param stream stream on which the operator should be applied
   * @return the stream with this matching operator applied
   */
  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    var preProcessedStream = preProcessAndApplyWindow(stream);
    return preProcessedStream.process(new DualSimulationProcess<>(queryGraph));
  }
}
