package streaming.operators;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.flink.util.Collector;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.Vertex;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.ElementGroupingInformation;

public class VertexAggregationFunction implements GraphElementAggregationFunctionI {

  private ElementGroupingInformation vertexEgi;
  private AggregationMapping aggregationMapping;
  private AggregateMode aggregateMode;

  public VertexAggregationFunction(ElementGroupingInformation vertexEgi,
      AggregationMapping aggregationMapping, AggregateMode aggregateMode) {
    this.vertexEgi = vertexEgi;
    this.aggregationMapping = aggregationMapping;
    this.aggregateMode = aggregateMode;
  }

  @Override
  public void flatMap(Collection<Edge> edgeSet, Collector<Edge> out) {
    GraphElementInformation aggregatedGei = new GraphElementInformation();
    Function<Edge, Vertex> getVertex =
        aggregateMode.equals(AggregateMode.SOURCE) ? Edge::getSource : Edge::getTarget;
    for (Edge e : edgeSet) {
      GraphElementInformation vertexGei = getVertex.apply(e).getGei();
      aggregateGei(aggregationMapping, vertexEgi, aggregatedGei, vertexGei);
    }
    BiFunction<Vertex, Edge, Edge> generateNewEdge = aggregateMode.equals(AggregateMode.SOURCE)
        ? (v, e) -> new Edge(v, e.getTarget(), e.getGei())
        : (v, e) -> new Edge(e.getSource(), v, e.getGei());
    for (Edge e : edgeSet) {
      if (!e.isReverse()) {
        Vertex aggregatedVertex = new Vertex(aggregatedGei);
        Edge aggregatedEdge = generateNewEdge.apply(aggregatedVertex, e);
        out.collect(aggregatedEdge);
      } else {
        out.collect(e);
      }
    }
  }
}