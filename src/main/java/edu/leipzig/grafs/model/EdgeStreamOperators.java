package edu.leipzig.grafs.model;

import edu.leipzig.grafs.operators.interfaces.OperatorI;
import edu.leipzig.grafs.operators.subgraph.Subgraph;
import edu.leipzig.grafs.operators.subgraph.Subgraph.Strategy;
import edu.leipzig.grafs.operators.transform.EdgeTransformation;
import edu.leipzig.grafs.operators.transform.VertexTransformation;
import edu.leipzig.grafs.operators.union.DisjunctUnion;
import edu.leipzig.grafs.operators.union.UnionWithDuplicateInWindow;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public interface EdgeStreamOperators {

  EdgeStream callForStream(OperatorI operator);

  default EdgeStream vertexInducedSubgraph(
      FilterFunction<Vertex> vertexGeiPredicate) {
  default EdgeStream dualSimulation(String gdlQueryStr, WindowAssigner<Object, Window> window) {
    return callForStream(new DualSimulation<>(gdlQueryStr, window));
  }

  default EdgeStream isomorphismMatching(String gdlQueryStr,
      WindowAssigner<Object, Window> window) {
    return callForStream(new Isomorphism<>(gdlQueryStr, window));
  }
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

  default EdgeStream transformEdges(MapFunction<Edge, Edge> mapper) {
    return callForStream(new EdgeTransformation(mapper));
  }

  default EdgeStream transformVertices(MapFunction<Vertex, Vertex> mapper) {
    return callForStream(new VertexTransformation(mapper));
  }

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
