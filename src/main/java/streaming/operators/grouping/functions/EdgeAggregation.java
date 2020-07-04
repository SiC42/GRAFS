package streaming.operators.grouping.functions;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.factory.EdgeFactory;
import streaming.model.EdgeContainer;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.AggregationVertex;
import streaming.operators.grouping.model.GroupingInformation;

public class EdgeAggregation implements EdgeAggregationI {


  private final GroupingInformation vertexGroupInfo;
  private final AggregationMapping vertexAggregationMapping;
  private final GroupingInformation edgeGroupInfo;
  private final AggregationMapping edgeAggregationMapping;

  public EdgeAggregation(GroupingInformation vertexGroupInfo,
      AggregationMapping vertexAggregationMapping, GroupingInformation edgeGroupInfo,
      AggregationMapping edgeAggregationMapping) {
    checkAggregationAndGroupingKeyIntersection(vertexAggregationMapping, vertexGroupInfo);
    if (edgeAggregationMapping != null && edgeGroupInfo != null) {
      checkAggregationAndGroupingKeyIntersection(edgeAggregationMapping, edgeGroupInfo);
    }
    this.vertexGroupInfo = vertexGroupInfo;
    this.vertexAggregationMapping = vertexAggregationMapping;
    this.edgeGroupInfo = edgeGroupInfo;
    this.edgeAggregationMapping = edgeAggregationMapping;
  }


  @Override
  public void apply(String s, TimeWindow window, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> out) {
    var aggregatedSource = new AggregationVertex();
    var aggregatedTarget = new AggregationVertex();
    var aggregatedEdge = new EdgeFactory()
        .createEdge(aggregatedSource.getId(), aggregatedTarget.getId());
    int count = 0;

    EdgeContainer lastEc = null;
    for (var ec : ecIterable) {
      aggregatedSource = aggregateVertex(vertexAggregationMapping,
          vertexGroupInfo,
          aggregatedSource,
          ec.getSourceVertex());
      aggregatedTarget = aggregateVertex(vertexAggregationMapping,
          vertexGroupInfo,
          aggregatedTarget,
          ec.getTargetVertex());
      aggregatedEdge = aggregateEdge(edgeAggregationMapping, edgeGroupInfo,
          aggregatedEdge,
          ec.getEdge());
      count++;
      lastEc = ec;
    }
    EdgeContainer aggregatedEContainer;
    if (count > 1) { // No need for aggregation when only one edge was "aggregated"
      aggregatedEContainer = new EdgeContainer(aggregatedEdge, aggregatedSource,
          aggregatedTarget);
    } else {
      aggregatedEContainer = lastEc;
    }

    out.collect(aggregatedEContainer);

  }


}
