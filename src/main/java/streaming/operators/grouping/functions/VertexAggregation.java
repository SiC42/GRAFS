package streaming.operators.grouping.functions;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.flink.util.Collector;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.Vertex;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public class VertexAggregation implements GraphElementAggregationI {

  private final GroupingInformation vertexEgi;
  private final AggregationMapping aggregationMapping;
  private final AggregateMode aggregateMode;

  public VertexAggregation(GroupingInformation vertexEgi,
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
