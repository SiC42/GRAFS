package edu.leipzig.grafs.model;

import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.interfaces.OperatorI;
import edu.leipzig.grafs.operators.matching.DualSimulation;
import edu.leipzig.grafs.operators.matching.Isomorphism;
import edu.leipzig.grafs.operators.subgraph.Subgraph;
import edu.leipzig.grafs.operators.subgraph.Subgraph.Strategy;
import edu.leipzig.grafs.operators.transform.EdgeTransformation;
import edu.leipzig.grafs.operators.transform.VertexTransformation;
import edu.leipzig.grafs.operators.union.DisjunctUnion;
import edu.leipzig.grafs.operators.union.UnionWithDuplicateInWindow;
import java.util.Set;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public interface EdgeStreamOperators {

  EdgeStream callForStream(OperatorI operator);

  // ---------------------------------------------------------------------------
  //  Grouping operators
  // ---------------------------------------------------------------------------

  default EdgeStream grouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, Window> window) {
    return grouping(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions, window,
        null);
  }

  default EdgeStream grouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregationFunctions,
      WindowAssigner<Object, Window> window,
      Trigger<EdgeContainer, Window> trigger) {
    return callForStream(
        new Grouping<>(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregationFunctions, window,
            trigger));
  }

  // ---------------------------------------------------------------------------
  //  Matching operators
  // ---------------------------------------------------------------------------

  default EdgeStream dualSimulation(String gdlQueryStr, WindowAssigner<Object, Window> window) {
    return callForStream(new DualSimulation<>(gdlQueryStr, window));
  }

  default EdgeStream isomorphismMatching(String gdlQueryStr,
      WindowAssigner<Object, Window> window) {
    return callForStream(new Isomorphism<>(gdlQueryStr, window));
  }

  // ---------------------------------------------------------------------------
  //  Subgraph operators
  // ---------------------------------------------------------------------------

  default EdgeStream vertexInducedSubgraph(FilterFunction<Vertex> vertexGeiPredicate) {
    return callForStream(new Subgraph(vertexGeiPredicate, null, Strategy.VERTEX_INDUCED));
  }

  default EdgeStream edgeInducedSubgraph(FilterFunction<Edge> edgeGeiPredicate) {
    return callForStream(new Subgraph(null, edgeGeiPredicate, Strategy.EDGE_INDUCED));
  }

  default EdgeStream subgraph(FilterFunction<Vertex> vertexGeiPredicate,
      FilterFunction<Edge> edgeGeiPredicate) {
    return callForStream(new Subgraph(vertexGeiPredicate, edgeGeiPredicate, Strategy.BOTH));
  }

  default EdgeStream subgraph(FilterFunction<Vertex> vertexGeiPredicate,
      FilterFunction<Edge> edgeGeiPredicate, Strategy strategy) {
    return callForStream(new Subgraph(vertexGeiPredicate, edgeGeiPredicate, strategy));
  }

  // ---------------------------------------------------------------------------
  //  Transformation operators
  // ---------------------------------------------------------------------------

  default EdgeStream transformEdges(MapFunction<Edge, Edge> mapper) {
    return callForStream(new EdgeTransformation(mapper));
  }

  default EdgeStream transformVertices(MapFunction<Vertex, Vertex> mapper) {
    return callForStream(new VertexTransformation(mapper));
  }

  // ---------------------------------------------------------------------------
  //  Union operators
  // ---------------------------------------------------------------------------

  /**
   * Union of two or more edge streams creating a new stream containing all the elements from all
   * the streams.
   * <p>
   * Note: This operator assumes that the streams are disjunct, no element in both streams is
   * present in the other stream.
   *
   * @param streams The edge streams to union output with.
   * @return the unioned edge stream
   */
  default EdgeStream disjunctUnion(EdgeStream... streams) {
    return callForStream(new DisjunctUnion(streams));
  }

  default EdgeStream unionWithDuplicateInWindow(WindowAssigner<Object, Window> window,
      EdgeStream... streams) {
    return callForStream(new UnionWithDuplicateInWindow<>(window, streams));
  }

}
