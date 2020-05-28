package streaming.operators.grouping.functions;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.flink.util.Collector;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Element;
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
  public void flatMap(Collection<EdgeContainer> edgeSet, Collector<EdgeContainer> out) {
    Element aggregatedGei = new Element();
    Function<EdgeContainer, Vertex> getVertexFunction =
        aggregateMode.equals(AggregateMode.SOURCE) ? EdgeContainer::getSourceVertex
            : EdgeContainer::getTargetVertex;
    for (EdgeContainer ec : edgeSet) {
      Element vertexGei = getVertexFunction.apply(ec);
      aggregateGei(aggregationMapping, vertexEgi, aggregatedGei, vertexGei);
    }
    BiFunction<Vertex, EdgeContainer, EdgeContainer> generateUpdatedECFunction = aggregateMode.equals(AggregateMode.SOURCE)
        ? (v, ec) -> new EdgeContainer(ec.getEdge(), v, ec.getTargetVertex())
        : (v, ec) -> new EdgeContainer(ec.getEdge(), ec.getSourceVertex(), v);
    for (EdgeContainer ec : edgeSet) {
      if (!ec.getEdge().isReverse()) {
        Vertex aggregatedVertex = new Vertex(aggregatedGei);
        EdgeContainer aggregatedEdge = generateUpdatedECFunction.apply(aggregatedVertex, ec);
        out.collect(aggregatedEdge);
      } else {
        out.collect(ec);
      }
    }
  }
}
