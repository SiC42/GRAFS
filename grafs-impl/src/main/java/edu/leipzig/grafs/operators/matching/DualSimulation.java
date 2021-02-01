package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.BasicTriplet;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.model.window.WindowsI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.matching.logic.DualSimulationProcess;
import edu.leipzig.grafs.operators.matching.logic.FilterCandidates;
import edu.leipzig.grafs.operators.matching.model.Query;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import java.util.ArrayList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Base for all pattern matching operators. Pre-processes the stream by filtering the elements and
 * applying a window.
 */
public class DualSimulation implements
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
  public DualSimulation(String query, boolean withEdgeOrder,
      ArrayList<String> variableOrder) {
    gdlQuery = new Query(query, withEdgeOrder, variableOrder);
  }


  private DataStream<BasicTriplet<QueryVertex, QueryEdge>> filterExactEdgesWithVertices(
      DataStream<BasicTriplet<QueryVertex, QueryEdge>> stream) {
    return stream.filter(new FilterCandidates(gdlQuery.toTriplets(), false, null))
        .name("Filter relevant graph elements");
  }

  @Override
  public <FW extends Window> DataStream<Triplet> execute(
      DataStream<Triplet> stream, WindowingInformation<FW> wi) {
    var transformedStream = stream.map(
        new MapFunction<Triplet, BasicTriplet<QueryVertex, QueryEdge>>() {
          @Override
          public BasicTriplet<QueryVertex, QueryEdge> map(Triplet triplet)
              throws Exception {
            var e = triplet.getEdge();
            var qEdge = new QueryEdge(e.getId(), e.getLabel(), e.getSourceId(), e.getTargetId(),
                e.getProperties(), e.getGraphIds());
            var s = triplet.getSourceVertex();
            var qSource = new QueryVertex(s.getId(), s.getLabel(), s.getProperties(),
                s.getGraphIds());
            var t = triplet.getSourceVertex();
            var qTarget = new QueryVertex(t.getId(), t.getLabel(), t.getProperties(),
                t.getGraphIds());
            return new BasicTriplet<>(qEdge, qSource, qTarget);
          }
        }
    );
    var filteredStream = filterExactEdgesWithVertices(transformedStream);
    var windowedStream = filteredStream.windowAll(wi.getWindow());
    return windowedStream.process(new DualSimulationProcess<>(gdlQuery));
  }
}
