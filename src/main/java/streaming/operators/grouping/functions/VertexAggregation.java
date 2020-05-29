package streaming.operators.grouping.functions;

import java.util.function.BiFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.model.EdgeContainer;
import streaming.model.Element;
import streaming.model.Vertex;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public class VertexAggregation implements GraphElementAggregationI {

  private final GroupingInformation vertexGroupingInformation;
  private final AggregationMapping aggregationMapping;
  private final AggregateMode aggregateMode;

  public VertexAggregation(GroupingInformation vertexGroupingInformation,
      AggregationMapping aggregationMapping, AggregateMode aggregateMode) {
    this.vertexGroupingInformation = vertexGroupingInformation;
    this.aggregationMapping = aggregationMapping;
    this.aggregateMode = aggregateMode;
  }

  @Override
  public void apply(String s, TimeWindow window, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> out) {
    Element aggregatedElement = new Element();
    for (EdgeContainer ec : ecIterable) {
      Element vertexElement;
      if (aggregateMode.equals(AggregateMode.SOURCE)) {
        vertexElement = ec.getSourceVertex();
      } else {
        vertexElement = ec.getTargetVertex();
      }
      aggregatedElement = aggregateElement(aggregationMapping, vertexGroupingInformation,
          aggregatedElement, vertexElement);
    }
    BiFunction<Vertex, EdgeContainer, EdgeContainer> generateUpdatedECFunction =
        aggregateMode.equals(AggregateMode.SOURCE)
            ? (v, ec) -> new EdgeContainer(ec.getEdge(), v, ec.getTargetVertex())
            : (v, ec) -> new EdgeContainer(ec.getEdge(), ec.getSourceVertex(), v);
    for (EdgeContainer ec : ecIterable) {
      if (!ec.getEdge().isReverse()) {
        Vertex aggregatedVertex = new Vertex(aggregatedElement);
        EdgeContainer aggregatedEdge = generateUpdatedECFunction.apply(aggregatedVertex, ec);
        out.collect(aggregatedEdge);
      } else {
        out.collect(ec);
      }
    }
  }

}
