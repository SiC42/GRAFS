package streaming.operators.grouping.functions;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.model.EdgeContainer;
import streaming.model.Element;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public class EdgeAggregation implements GraphElementAggregationI {


  private final GroupingInformation vertexGroupInfo;
  private final AggregationMapping vertexAggregationMapping;
  private final GroupingInformation edgeGroupInfo;
  private final AggregationMapping edgeAggregationMapping;

  public EdgeAggregation(GroupingInformation vertexGroupInfo,
      AggregationMapping vertexAggregationMapping, GroupingInformation edgeGroupInfo,
      AggregationMapping edgeAggregationMapping) {
    checkAggregationAndGroupingKeyIntersection(vertexAggregationMapping, vertexGroupInfo);
    checkAggregationAndGroupingKeyIntersection(edgeAggregationMapping, edgeGroupInfo);
    this.vertexGroupInfo = vertexGroupInfo;
    this.vertexAggregationMapping = vertexAggregationMapping;
    this.edgeGroupInfo = edgeGroupInfo;
    this.edgeAggregationMapping = edgeAggregationMapping;
  }


  @Override
  public void apply(String s, TimeWindow window, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> out) {
    Element aggregatedSource = new Element();
    Element aggregatedTarget = new Element();
    Element aggregatedEdge = new Element();

    for (EdgeContainer e : ecIterable) {
      aggregateElement(vertexAggregationMapping, vertexGroupInfo, aggregatedSource,
          e.getSourceVertex());
      aggregateElement(vertexAggregationMapping, vertexGroupInfo, aggregatedTarget,
          e.getTargetVertex());
      aggregateElement(edgeAggregationMapping, edgeGroupInfo, aggregatedEdge, e.getEdge());
    }
    EdgeContainer aggregatedEContainer = new EdgeContainer(aggregatedEdge, aggregatedSource,
        aggregatedTarget);
    out.collect(aggregatedEContainer);

  }


}
