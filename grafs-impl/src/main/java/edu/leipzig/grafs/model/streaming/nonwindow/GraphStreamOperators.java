package edu.leipzig.grafs.model.streaming.nonwindow;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphToGraphOperatorI;
import edu.leipzig.grafs.operators.subgraph.Subgraph;
import edu.leipzig.grafs.operators.subgraph.Subgraph.Strategy;
import edu.leipzig.grafs.operators.transform.EdgeTransformation;
import edu.leipzig.grafs.operators.transform.VertexTransformation;
import edu.leipzig.grafs.operators.union.DisjunctUnion;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Defines the operators that are available on a {@link Triplet}.
 */
public interface GraphStreamOperators {

  /**
   * Creates a graph stream using the given operator.
   *
   * @param operator operator that should be used on this graph stream
   * @return result of given operator as graph stream
   */
  GraphStream callForGraph(GraphToGraphOperatorI operator);


  /**
   * Creates a graph collection stream using the given operator.
   *
   * @param operator operator that should be used on this graph stream
   * @return result of given operator as graph collection stream
   */
  GCStream callForGC(GraphToGraphCollectionOperatorI operator);

  // ---------------------------------------------------------------------------
  //  Operators
  // ---------------------------------------------------------------------------

  /**
   * Creates an triplet stream with the {@link Subgraph} operator applied. The strategy to use vertex
   * induced subgraphing is applied.
   *
   * @param vertexPredicate vertex filter used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default GraphStream vertexInducedSubgraph(FilterFunction<Vertex> vertexPredicate) {
    return this.callForGraph(new Subgraph(vertexPredicate, null, Strategy.VERTEX_INDUCED));
  }

  /**
   * Creates an triplet stream with the {@link Subgraph} operator applied. The strategy to use edge
   * induced subgraphing is applied.
   *
   * @param edgePredicate edge filter used for the subgraph operator
   * @return result stream of the subgraph operator
   */
  default GraphStream edgeInducedSubgraph(FilterFunction<Edge> edgePredicate) {
    return this.callForGraph(new Subgraph(null, edgePredicate, Strategy.EDGE_INDUCED));
  }

  /**
   * Creates an triplet stream with the {@link Subgraph} operator applied. Filters on both vertices and
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
   * Creates an triplet stream with the {@link Subgraph} operator applied.
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

  /**
   * Creates an triplet stream with the {@link EdgeTransformation} operator applied.
   *
   * @param mapper edge mapping used in the transformation operator
   * @return result stream of the transformation operator
   */
  default GraphStream transformEdges(MapFunction<Element, Element> mapper) {
    return this.callForGraph(new EdgeTransformation(mapper));
  }

  /**
   * Creates an triplet stream with the {@link VertexTransformation} operator applied.
   *
   * @param mapper vertex mapping used in the transformation operator
   * @return result stream of the transformation operator
   */
  default GraphStream transformVertices(MapFunction<Element, Element> mapper) {
    return this.callForGraph(new VertexTransformation(mapper));
  }

  /**
   * Creates an triplet stream with the {@link DisjunctUnion} operator applied. Represents a union of
   * two or more triplet streams creating a new stream containing all the elements from all the
   * streams.
   * <p>
   * Note: This operator assumes that the streams are disjunct, i.e. no element in both streams is
   * present in the other stream.
   *
   * @param streams the triplet streams to union output with
   * @return the unioned triplet stream
   */
  default GraphStream disjunctUnion(GraphStream... streams) {
    return this.callForGraph(new DisjunctUnion(streams));
  }


}
