package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class EdgeAggregation<W extends Window> extends ElementAggregationProcess<W> {


  private final GroupingInformation edgeGroupInfo;
  private final Set<AggregateFunction> edgeAggregateFunctions;

  public EdgeAggregation(GroupingInformation edgeGroupInfo,
      Set<AggregateFunction> edgeAggregateFunctions) {
    if (edgeAggregateFunctions != null && edgeGroupInfo != null) {
      checkAggregationAndGroupingKeyIntersection(edgeAggregateFunctions, edgeGroupInfo);
    }
    this.edgeGroupInfo = edgeGroupInfo;
    this.edgeAggregateFunctions = edgeAggregateFunctions;
  }

  @Override
  public void process(String s, Context context, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> out) {
    var aggregatedEdge = new Edge();

    EdgeContainer lastEc = null;

    for (var ec : ecIterable) {
      aggregatedEdge = (Edge) aggregateElement(aggregatedEdge, ec.getEdge(),
          edgeAggregateFunctions
      );
      lastEc = ec;
    }
    aggregatedEdge = (Edge) checkForMissingAggregationsAndApply(edgeAggregateFunctions,
        aggregatedEdge);
    EdgeContainer aggregatedEContainer;

    // we have not set the grouped properties yet
    assert lastEc != null;
    aggregatedEdge = (Edge) setGroupedProperties(edgeGroupInfo,
        aggregatedEdge,
        lastEc.getEdge());
    var source = lastEc.getSourceVertex();
    var target = lastEc.getTargetVertex();
    aggregatedEdge.setSourceId(source.getId());
    aggregatedEdge.setTargetId(target.getId());
    aggregatedEContainer = new EdgeContainer(aggregatedEdge, source,
        target);

    out.collect(aggregatedEContainer);

  }

}
