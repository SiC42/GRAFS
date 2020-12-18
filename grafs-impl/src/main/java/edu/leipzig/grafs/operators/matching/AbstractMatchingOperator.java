package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.matching.logic.EdgeQueryFilter;
import edu.leipzig.grafs.operators.matching.logic.VertexQueryFilter;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Base for all pattern matching operators. Pre-processes the stream by filtering the elements and applying the window.
 * @param <W> type of window that is used in this operation
 */
public abstract class AbstractMatchingOperator<W extends Window> implements
    GraphToGraphCollectionOperatorI {

  /**
   * Query graph used to find pattern matches
   */
  final Graph queryGraph;
  /**
   * Window that is applied to the stream before the pattern matching is applied.
   */
  final WindowAssigner<Object, W> window;
  /**
   * Trigger that is applied to the window.
   */
  final Trigger<EdgeContainer, W> trigger;

  /**
   * Initializes the operator with the given parameter
   * @param query query string that is used to make the query graph
   * @param window window that for this operation
   * @param trigger optional window trigger that is used for this operation
   */
  public AbstractMatchingOperator(String query, WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    queryGraph = AsciiGraphLoader.fromString(query).createGraph();
    this.window = window;
    this.trigger = trigger;
  }

  /**
   * Prepares edge stream to be used by the pattern matching process by filtering the elements to only use elements that fit the pattern and applies window to the stream.
   * @param stream stream that should be used to pre process
   * @return pre-processed stream ready for applying the pattern matching process
   */
  AllWindowedStream<EdgeContainer, W> preProcessAndApplyWindow(DataStream<EdgeContainer> stream) {
    var filteredStream = queryGraph.getEdges().isEmpty() ? // Only vertices?
        stream.filter(new VertexQueryFilter(queryGraph)) :
        stream.filter(new EdgeQueryFilter(queryGraph));
    var windowedStream = filteredStream.windowAll(window);
    if (trigger != null) {
      windowedStream = windowedStream.trigger(trigger);
    }
    return windowedStream;
  }

}
