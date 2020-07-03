package streaming.model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import streaming.operators.OperatorI;
import streaming.operators.grouping.Grouping;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;
import streaming.operators.subgraph.Subgraph;
import streaming.operators.subgraph.Subgraph.Strategy;
import streaming.operators.transform.EdgeTransformation;
import streaming.operators.transform.VertexTransformation;

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

  default EdgeStream transformEdge(MapFunction<Edge, Edge> mapper) {
    return callForStream(new EdgeTransformation(mapper));
  }

  default EdgeStream transformVertices(MapFunction<Vertex, Vertex> mapper) {
    return callForStream(new VertexTransformation(mapper));
  }

}
