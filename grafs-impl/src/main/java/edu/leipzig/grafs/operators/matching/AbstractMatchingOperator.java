package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.model.window.WindowsI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.matching.logic.EdgeQueryFilter;
import edu.leipzig.grafs.operators.matching.logic.VertexQueryFilter;
import edu.leipzig.grafs.operators.matching.model.Query;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Base for all pattern matching operators. Pre-processes the stream by filtering the elements and
 * applying a window.
 */
public abstract class AbstractMatchingOperator implements
    WindowedGraphToGraphCollectionOperatorI<WindowsI<? extends Window>> {

  /**
   * Query graph used to find pattern matches
   */
  final Query gdlQuery;

  /**
   * Initializes the operator with the given parameter
   *
   * @param query query string that is used to make the query graph
   */
  public AbstractMatchingOperator(String query) {
    gdlQuery = new Query(query, false, );

  }

  /**
   * Prepares triplet stream to be used by the pattern matching process by filtering the elements to
   * only use elements that fit the pattern and applies window to the stream.
   *
   * @param stream stream that should be used to pre process
   * @return pre-processed stream ready for applying the pattern matching process
   */
  <W extends Window> AllWindowedStream<Triplet<Vertex,Edge>, W> preProcessAndApplyWindow(
      DataStream<Triplet<Vertex,Edge>> stream, WindowingInformation<W> wi) {
    var transformedStream = stream.map(
        new MapFunction<Triplet<Vertex, Edge>, Triplet<QueryVertex,QueryEdge>>() {
          @Override
          public Triplet<QueryVertex, QueryEdge> map(Triplet<Vertex, Edge> triplet)
              throws Exception {
            var e = triplet.getEdge();
            var qEdge = new QueryEdge(e.getId(), e.getLabel(), e.getSourceId(), e.getTargetId(), e.getProperties(), e.getGraphIds());
            var s = triplet.getSourceVertex();
            var qSource = new QueryVertex(s.getId(), s.getLabel(), s.getProperties(), s.getGraphIds());
            var t = triplet.getSourceVertex();
            var qTarget = new QueryVertex(t.getId(), t.getLabel(), t.getProperties(), t.getGraphIds());
            return new Triplet<>(qEdge, qSource, qTarget);
          }
        }
    );
    var filteredStream = queryGraph.getEdges().isEmpty() ? // Only vertices?
        transformedStream.filter(new VertexQueryFilter(queryGraph)) :
        transformedStream.filter(new EdgeQueryFilter(queryGraph));
    filteredStream = filteredStream.name("Filter relevant graph elements");
    var windowedStream = filteredStream.windowAll(wi.getWindow());
    windowedStream = applyOtherWindowInformation(windowedStream, wi);
    return windowedStream;
  }

}
