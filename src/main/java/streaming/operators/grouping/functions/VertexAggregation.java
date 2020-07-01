package streaming.operators.grouping.functions;

import java.util.function.BiFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.factory.VertexFactory;
import streaming.model.EdgeContainer;
import streaming.model.GraphElement;
import streaming.model.Vertex;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public class VertexAggregation implements GraphElementAggregationI {

  private final GroupingInformation vertexGroupInfo;
  private final AggregationMapping aggregationMapping;
  private final AggregateMode aggregateMode;

  public VertexAggregation(GroupingInformation vertexGroupInfo,
      AggregationMapping aggregationMapping, AggregateMode aggregateMode) {
    checkAggregationAndGroupingKeyIntersection(aggregationMapping, vertexGroupInfo);
    this.vertexGroupInfo = vertexGroupInfo;
    this.aggregationMapping = aggregationMapping;
    this.aggregateMode = aggregateMode;
  }

  @Override
  public void apply(String s, TimeWindow window, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> out) {
    Vertex aggregatedVertex = new VertexFactory().createVertex();
    for (EdgeContainer ec : ecIterable) {
      GraphElement vertexElement;
      if (aggregateMode.equals(AggregateMode.SOURCE)) {
        vertexElement = ec.getSourceVertex();
      } else {
        vertexElement = ec.getTargetVertex();
      }
      aggregatedVertex = (Vertex) aggregateGraphElement(aggregationMapping, vertexGroupInfo,
          aggregatedVertex, vertexElement);
    }
    BiFunction<Vertex, EdgeContainer, EdgeContainer> generateUpdatedECFunction =
        aggregateMode.equals(AggregateMode.SOURCE)
            ? (v, ec) -> new EdgeContainer(ec.getEdge(), v, ec.getTargetVertex())
            : (v, ec) -> new EdgeContainer(ec.getEdge(), ec.getSourceVertex(), v);
    for (EdgeContainer ec : ecIterable) {
      if (!ec.getEdge().isReverse()) {
        Vertex finalVertex = new VertexFactory().createVertex(aggregatedVertex);
        EdgeContainer aggregatedEdge = generateUpdatedECFunction.apply(finalVertex, ec);
        out.collect(aggregatedEdge);
      } else {
        out.collect(ec);
      }
    }
  }

}
