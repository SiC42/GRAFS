package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
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
 * Defines the operators that are available on a {@link Triplet}.
 */
public interface GraphStreamOperators{
  /**
   * Creates an edge stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  GraphStream callForGraph(GraphToGraphOperatorI operator);

  GCStream callForGC(GraphToGraphCollectionOperatorI operator);

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
  default GraphStream vertexInducedSubgraph(FilterFunction<Vertex> vertexPredicate) {
    return this.callForGraph(new Subgraph(vertexPredicate, null, Strategy.VERTEX_INDUCED));
  }

  /**
   * Creates an edge stream with the {@link Subgraph} operator applied. The strategy to use edge
   * induced subgraphing is applied.
   *
   * @param edgePredicate edge filter used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default GraphStream edgeInducedSubgraph(FilterFunction<Edge> edgePredicate) {
    return this.callForGraph(new Subgraph(null, edgePredicate, Strategy.EDGE_INDUCED));
  }

  /**
   * Creates an edge stream with the {@link Subgraph} operator applied. Filters on both vertices and
   * edges;
   *
   * @param vertexPredicate vertex filter used for the subgraph operator
   * @param edgePredicate   edge filter used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default GraphStream subgraph(FilterFunction<Vertex> vertexPredicate,
      FilterFunction<Edge> edgePredicate) {
    return this.callForGraph(new Subgraph(vertexPredicate, edgePredicate, Strategy.BOTH));
  }

  /**
   * Creates an edge stream with the {@link Subgraph} operator applied.
   *
   * @param vertexPredicate vertex filter used for the subgraph operator
   * @param edgePredicate   edge filter used for the subgraph operator
   * @param strategy        strategy used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default GraphStream subgraph(FilterFunction<Vertex> vertexPredicate,
      FilterFunction<Edge> edgePredicate, Strategy strategy) {
    return this.callForGraph(new Subgraph(vertexPredicate, edgePredicate, strategy));
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
  default GraphStream transformEdges(MapFunction<Edge, Edge> mapper) {
    return this.callForGraph(new EdgeTransformation(mapper));
  }

  /**
   * Creates an edge stream with the {@link VertexTransformation} operator applied.
   *
   * @param mapper vertex mapping used in the transformation operator
   * @return result stream of the transformation operator
   */
  default GraphStream transformVertices(MapFunction<Vertex, Vertex> mapper) {
    return this.callForGraph(new VertexTransformation(mapper));
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
  default GraphStream disjunctUnion(GraphStream... streams) {
    return this.callForGraph(new DisjunctUnion(streams));
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
  default GraphStream unionWithDuplicateInWindow(WindowAssigner<Object, Window> window,
      GraphStream... streams) {
    return this.callForGraph(new UnionWithDuplicateInWindow<>(window, streams));
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
  default GraphStream unionWithDuplicateInWindow(WindowAssigner<Object, Window> window,
      Trigger<Triplet, Window> trigger,
      GraphStream... streams) {
    return this.callForGraph(new UnionWithDuplicateInWindow<>(window, trigger, streams));
  }

}