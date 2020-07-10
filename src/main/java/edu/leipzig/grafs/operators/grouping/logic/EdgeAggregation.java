package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class EdgeAggregation implements GraphElementAggregationProcess, VertexAggregationProcess {


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
    var aggregatedSource = new AggregatedVertex();
    var aggregatedTarget = new AggregatedVertex();
    var aggregatedEdge = new EdgeFactory()
        .createEdge(aggregatedSource.getId(), aggregatedTarget.getId());

    int count = 0;
    EdgeContainer lastEc = null;

    for (var ec : ecIterable) {
      aggregatedSource = aggregateVertex(vertexAggregationMapping, aggregatedSource,
          ec.getSourceVertex());
      aggregatedTarget = aggregateVertex(vertexAggregationMapping, aggregatedTarget,
          ec.getTargetVertex());
      aggregatedEdge = (Edge) aggregateGraphElement(edgeAggregationMapping, aggregatedEdge,
          ec.getEdge());
      count++;
      lastEc = ec;
    }
    EdgeContainer aggregatedEContainer;

    // No need for aggregation when only one edge was "aggregated"
    if (count > 1) {
      // we have not set the grouped properties yet
      aggregatedSource = (AggregatedVertex) setGroupedProperties(vertexGroupInfo,
          aggregatedSource,
          lastEc.getSourceVertex());
      aggregatedTarget = (AggregatedVertex) setGroupedProperties(vertexGroupInfo,
          aggregatedTarget,
          lastEc.getTargetVertex());
      aggregatedEdge = (Edge) setGroupedProperties(edgeGroupInfo,
          aggregatedEdge,
          lastEc.getEdge());
      aggregatedEContainer = new EdgeContainer(aggregatedEdge, aggregatedSource,
          aggregatedTarget);
    } else {
      aggregatedEContainer = lastEc;
    }

    out.collect(aggregatedEContainer);

  }


}
