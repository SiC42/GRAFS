package streaming.operators.grouping.functions;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import streaming.model.EdgeContainer;
import streaming.model.Element;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public class EdgeAggregation implements GraphElementAggregationI {


  private final GroupingInformation vertexEgi;
  private final AggregationMapping vertexAggregationMapping;
  private final GroupingInformation edgeEgi;
  private final AggregationMapping edgeAggregationMapping;

  public EdgeAggregation(GroupingInformation vertexEgi,
      AggregationMapping vertexAggregationMapping, GroupingInformation edgeEgi,
      AggregationMapping edgeAggregationMapping) {
    this.vertexEgi = vertexEgi;
    this.vertexAggregationMapping = vertexAggregationMapping;
    this.edgeEgi = edgeEgi;
    this.edgeAggregationMapping = edgeAggregationMapping;
  }


  @Override
  public void apply(String s, TimeWindow window, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> out) {
    Element aggregatedSource = new Element();
    Element aggregatedTarget = new Element();
    Element aggregatedEdge = new Element();

    for (EdgeContainer e : ecIterable) {
      aggregateElement(vertexAggregationMapping, vertexEgi, aggregatedSource,
          e.getSourceVertex());
      aggregateElement(vertexAggregationMapping, vertexEgi, aggregatedTarget,
          e.getTargetVertex());
      aggregateElement(edgeAggregationMapping, edgeEgi, aggregatedEdge, e.getEdge());
    }
    EdgeContainer aggregatedEContainer = new EdgeContainer(aggregatedEdge, aggregatedSource,
        aggregatedTarget);
    out.collect(aggregatedEContainer);

  }


}
