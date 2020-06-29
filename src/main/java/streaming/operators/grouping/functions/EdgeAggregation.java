package streaming.operators.grouping.functions;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Vertex;
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
    Vertex aggregatedSource = new Vertex();
    Vertex aggregatedTarget = new Vertex();
    Edge aggregatedEdge = new Edge();

    for (EdgeContainer e : ecIterable) {
      aggregatedSource = (Vertex) aggregateGraphElement(vertexAggregationMapping, vertexGroupInfo,
          aggregatedSource,
          e.getSourceVertex());
      aggregatedTarget = (Vertex) aggregateGraphElement(vertexAggregationMapping, vertexGroupInfo,
          aggregatedTarget,
          e.getTargetVertex());
      aggregatedEdge = (Edge) aggregateGraphElement(edgeAggregationMapping, edgeGroupInfo,
          aggregatedEdge,
          e.getEdge());
    }
    EdgeContainer aggregatedEContainer = new EdgeContainer(aggregatedEdge, aggregatedSource,
        aggregatedTarget);
    out.collect(aggregatedEContainer);

  }


}
