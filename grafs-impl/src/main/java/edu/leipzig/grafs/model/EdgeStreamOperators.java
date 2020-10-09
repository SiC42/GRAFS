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

/**
 * Defines the operators that are available on a {@link EdgeContainer}.
 */
public interface EdgeStreamOperators {

  /**
   * Creates an edge stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  EdgeStream callForStream(OperatorI operator);

  // ---------------------------------------------------------------------------
  //  Grouping operators
  // ---------------------------------------------------------------------------

  /**
   * Creates an edge stream with the {@link Grouping) operator applied. Uses no trigger
   *
   * @param vertexGi                 vertex grouping information used for the operation
   * @param vertexAggregateFunctions vertex aggregation functions used for the operation
   * @param edgeGi                   edge grouping information used for the operation
   * @param edgeAggregateFunctions   edge aggregation functions used for the operation
   * @param window                   window on which the operation should be applied on
   * @return result stream of the grouping operator
   */
  default EdgeStream grouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, Window> window) {
    return grouping(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions, window,
        null);
  }

  /**
   * Creates an edge stream with the {@link Grouping) operator applied.
   *
   * @param vertexGi                 vertex grouping information used for the operation
   * @param vertexAggregateFunctions vertex aggregation functions used for the operation
   * @param edgeGi                   edge grouping information used for the operation
   * @param edgeAggregateFunctions   edge aggregation functions used for the operation
   * @param window                   window on which the operation should be applied on
   * @param trigger                  trigger which should be applied to end the window for the
   *                                 operation
   * @return result stream of the grouping operator
   */
  default EdgeStream grouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, Window> window,
      Trigger<EdgeContainer, Window> trigger) {
    return callForStream(
        new Grouping<>(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions, window,
            trigger));
  }

  // ---------------------------------------------------------------------------
  //  Matching operators
  // ---------------------------------------------------------------------------

  /**
   * Creates an edge stream with the {@link DualSimulation} operator applied. Uses no trigger.
   *
   * @param gdlQueryStr query pattern with which the stream is matched
   * @param window      window on which the operation should be applied on
   * @return result stream of the matching operator
   */
  default EdgeStream dualSimulation(String gdlQueryStr, WindowAssigner<Object, Window> window) {
    return callForStream(new DualSimulation<>(gdlQueryStr, window));
  }

  /**
   * Creates an edge stream with the {@link DualSimulation} operator applied.
   *
   * @param gdlQueryStr query pattern with which the stream is matched
   * @param window      window on which the operation should be applied on
   * @param trigger     trigger which should be applied to end the window for the operation
   * @return result stream of the matching operator
   */
  default EdgeStream dualSimulation(String gdlQueryStr, WindowAssigner<Object, Window> window,
      Trigger<EdgeContainer, Window> trigger) {
    return callForStream(new DualSimulation<>(gdlQueryStr, window));
  }

  /**
   * Creates an edge stream with the {@link Isomorphism} matching operator applied. Uses no
   * trigger.
   *
   * @param gdlQueryStr query pattern with which the stream is matched
   * @param window      window on which the operation should be applied on
   * @return result stream of the matching operator
   */
  default EdgeStream isomorphismMatching(String gdlQueryStr,
      WindowAssigner<Object, Window> window) {
    return callForStream(new Isomorphism<>(gdlQueryStr, window));
  }


  /**
   * Creates an edge stream with the {@link Isomorphism} matching operator applied.
   *
   * @param gdlQueryStr query pattern with which the stream is matched
   * @param window      window on which the operation should be applied on
   * @param trigger     trigger which should be applied to end the window for the operation
   * @return result stream of the matching operator
   */
  default EdgeStream isomorphismMatching(String gdlQueryStr, WindowAssigner<Object, Window> window,
      Trigger<EdgeContainer, Window> trigger) {
    return callForStream(new Isomorphism<>(gdlQueryStr, window));
  }

  // ---------------------------------------------------------------------------
  //  Subgraph operators
  // ---------------------------------------------------------------------------

  /**
   * Creates an edge stream with the {@link Subgraph} operator applied. The strategy to use vertex
   * induced subgraphing is applied.
   *
   * @param vertexPredicate vertex filter used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default EdgeStream vertexInducedSubgraph(FilterFunction<Vertex> vertexPredicate) {
    return callForStream(new Subgraph(vertexPredicate, null, Strategy.VERTEX_INDUCED));
  }

  /**
   * Creates an edge stream with the {@link Subgraph} operator applied. The strategy to use edge
   * induced subgraphing is applied.
   *
   * @param edgePredicate edge filter used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default EdgeStream edgeInducedSubgraph(FilterFunction<Edge> edgePredicate) {
    return callForStream(new Subgraph(null, edgePredicate, Strategy.EDGE_INDUCED));
  }

  /**
   * Creates an edge stream with the {@link Subgraph} operator applied. Filters on both vertices and
   * edges;
   *
   * @param vertexPredicate vertex filter used for the subgraph operator
   * @param edgePredicate   edge filter used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default EdgeStream subgraph(FilterFunction<Vertex> vertexPredicate,
      FilterFunction<Edge> edgePredicate) {
    return callForStream(new Subgraph(vertexPredicate, edgePredicate, Strategy.BOTH));
  }

  /**
   * Creates an edge stream with the {@link Subgraph} operator applied.
   *
   * @param vertexPredicate vertex filter used for the subgraph operator
   * @param edgePredicate   edge filter used for the subgraph operator
   * @param strategy        strategy used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default EdgeStream subgraph(FilterFunction<Vertex> vertexPredicate,
      FilterFunction<Edge> edgePredicate, Strategy strategy) {
    return callForStream(new Subgraph(vertexPredicate, edgePredicate, strategy));
  }

  // ---------------------------------------------------------------------------
  //  Transformation operators
  // ---------------------------------------------------------------------------

  /**
   * Creates an edge stream with the {@link EdgeTransformation} operator applied.
   *
   * @param mapper edge mapping used in the transformation operator
   * @return result stream of the transformation operator
   */
  default EdgeStream transformEdges(MapFunction<Edge, Edge> mapper) {
    return callForStream(new EdgeTransformation(mapper));
  }

  /**
   * Creates an edge stream with the {@link VertexTransformation} operator applied.
   *
   * @param mapper vertex mapping used in the transformation operator
   * @return result stream of the transformation operator
   */
  default EdgeStream transformVertices(MapFunction<Vertex, Vertex> mapper) {
    return callForStream(new VertexTransformation(mapper));
  }

  // ---------------------------------------------------------------------------
  //  Union operators
  // ---------------------------------------------------------------------------

  /**
   * Creates an edge stream with the {@link DisjunctUnion} operator applied. Represents a union of
   * two or more edge streams creating a new stream containing all the elements from all the
   * streams.
   * <p>
   * Note: This operator assumes that the streams are disjunct, i.e. no element in both streams is
   * present in the other stream.
   *
   * @param streams the edge streams to union output with
   * @return the unioned edge stream
   */
  default EdgeStream disjunctUnion(EdgeStream... streams) {
    return callForStream(new DisjunctUnion(streams));
  }

  /**
   * Creates an edge stream with the {@link UnionWithDuplicateInWindow} operator applied. Union of
   * two or more edge streams creating a new stream containing all the elements from all the streams
   * with duplicates in a given window filtered out. No trigger is applied.
   *
   * @param window  windowed used in the union operator
   * @param streams the edge streams to union output with
   * @return the unioned edge stream
   */
  default EdgeStream unionWithDuplicateInWindow(WindowAssigner<Object, Window> window,
      EdgeStream... streams) {
    return callForStream(new UnionWithDuplicateInWindow<>(window, streams));
  }

  /**
   * Creates an edge stream with the {@link UnionWithDuplicateInWindow} operator applied. Union of
   * two or more edge streams creating a new stream containing all the elements from all the streams
   * with duplicates in a given window filtered out.
   *
   * @param window  windowed used in the union operator
   * @param streams the edge streams to union output with
   * @param trigger trigger which should be applied to end the window for the operation
   * @return the unioned edge stream
   */
  default EdgeStream unionWithDuplicateInWindow(WindowAssigner<Object, Window> window,
      Trigger<EdgeContainer, Window> trigger,
      EdgeStream... streams) {
    return callForStream(new UnionWithDuplicateInWindow<>(window, trigger, streams));
  }

}
