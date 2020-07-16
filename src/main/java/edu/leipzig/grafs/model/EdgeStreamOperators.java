package edu.leipzig.grafs.model;

import edu.leipzig.grafs.operators.OperatorI;
import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.subgraph.Subgraph;
import edu.leipzig.grafs.operators.subgraph.Subgraph.Strategy;
import edu.leipzig.grafs.operators.transform.EdgeTransformation;
import edu.leipzig.grafs.operators.transform.VertexTransformation;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;

public interface EdgeStreamOperators {

  EdgeStream callForStream(OperatorI operator);

  default EdgeStream vertexInducedSubgraph(
      FilterFunction<Vertex> vertexGeiPredicate) {
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

  default EdgeStream groupBy(GroupingInformation vertexEgi,
      AggregationMapping vertexAggregationFunctions,
      GroupingInformation edgeEgi, AggregationMapping edgeAggregationFunctions) {
    return callForStream(
        new Grouping(vertexEgi, vertexAggregationFunctions, edgeEgi, edgeAggregationFunctions));
  }

  default EdgeStream transformEdges(MapFunction<Edge, Edge> mapper) {
    return callForStream(new EdgeTransformation(mapper));
  }

  default EdgeStream transformVertices(MapFunction<Vertex, Vertex> mapper) {
    return callForStream(new VertexTransformation(mapper));
  }

  EdgeStream union(EdgeStream otherStream);

}
