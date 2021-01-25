package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.window.AbstractWindowedStream.WindowInformation;
import edu.leipzig.grafs.operators.interfaces.window.WindowGraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.matching.logic.EdgeQueryFilter;
import edu.leipzig.grafs.operators.matching.logic.VertexQueryFilter;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Base for all pattern matching operators. Pre-processes the stream by filtering the elements and
 * applying a window.
 */
public abstract class AbstractMatchingOperator implements
    WindowGraphToGraphCollectionOperatorI {

  /**
   * Query graph used to find pattern matches
   */
  final Graph queryGraph;

  /**
   * Initializes the operator with the given parameter
   *
   * @param query query string that is used to make the query graph
   */
  public AbstractMatchingOperator(String query) {
    queryGraph = AsciiGraphLoader.fromString(query).createGraph();

  }

  /**
   * Prepares triplet stream to be used by the pattern matching process by filtering the elements to
   * only use elements that fit the pattern and applies window to the stream.
   *
   * @param stream stream that should be used to pre process
   * @return pre-processed stream ready for applying the pattern matching process
   */
  <W extends Window> AllWindowedStream<Triplet, W> preProcessAndApplyWindow(
      DataStream<Triplet> stream, WindowInformation<W> wi) {
    var filteredStream = queryGraph.getEdges().isEmpty() ? // Only vertices?
        stream.filter(new VertexQueryFilter(queryGraph)) :
        stream.filter(new EdgeQueryFilter(queryGraph));
    filteredStream = filteredStream.name("Filter relevant graph elements");
    var windowedStream = filteredStream.windowAll(wi.getWindow());
    windowedStream = applyOtherWindowInformation(windowedStream, wi);
    return windowedStream;
  }

}
