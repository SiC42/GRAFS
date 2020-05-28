package streaming.operators.grouping.functions;

import java.util.Collection;
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
  public void flatMap(Collection<EdgeContainer> edgeSet, Collector<EdgeContainer> out) {
    Element aggregatedSource = new Element();
    Element aggregatedTarget = new Element();
    Element aggregatedEdge = new Element();

    for (EdgeContainer e : edgeSet) {
      aggregateGei(vertexAggregationMapping, vertexEgi, aggregatedSource,
          e.getSourceVertex());
      aggregateGei(vertexAggregationMapping, vertexEgi, aggregatedTarget,
          e.getTargetVertex());
      aggregateGei(edgeAggregationMapping, edgeEgi, aggregatedEdge, e.getEdge());
    }
    EdgeContainer aggregatedEContainer = new EdgeContainer(aggregatedEdge, aggregatedSource,
        aggregatedTarget);
    out.collect(aggregatedEContainer);

  }


}
