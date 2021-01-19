package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.AbstractWindowedStream.WindowInformation;
import edu.leipzig.grafs.operators.matching.logic.DualSimulationProcess;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * This operator represents a Dual Simulation on a window of the stream and is based on the
 * algorithm in <a href="https://ieeexplore.ieee.org/abstract/document/6906821">"DualIso: An
 * Algorithm for Subgraph Pattern Matching on Very Large Labeled Graphs"</a> by Saltz et al.
 */
public class DualSimulation extends AbstractMatchingOperator {

  /**
   * Initializes the operator with the given parameters.
   *
   * @param query query string that is used to make the query graph
   */
  public DualSimulation(String query) {
    super(query);
  }

  /**
   * Applies this operator on the stream and returns the stream with the operator applied.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with this matching operator applied
   */
  @Override
  public <W extends Window> DataStream<Triplet> execute(DataStream<Triplet> stream,
      WindowInformation<W> wi) {
    var preProcessedStream = preProcessAndApplyWindow(stream, wi);
    return preProcessedStream.process(new DualSimulationProcess<>(queryGraph))
        .name("Dual Simulation Pattern Matching");
  }
}
