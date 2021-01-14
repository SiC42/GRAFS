package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Provides the ability to aggregate on edges of the streams by providing the {@link
 * #process(String, Context, Iterable, Collector)} method.
 *
 * @param <W> the type of window to be used for the grouping
 */
public class EdgeAggregation<W extends Window> extends ElementAggregationProcess<W> {


  private final GroupingInformation edgeGroupInfo;
  private final Set<AggregateFunction> edgeAggregateFunctions;

  /**
   * Constructs the edge aggregation with the given information.
   *
   * @param edgeGroupInfo    grouping information used to determine which edges are in a group
   * @param aggregateFunctions aggregate functions that are used to calculate the aggregates and set
   *                           them in the aggregated edge
   */
  public EdgeAggregation(GroupingInformation edgeGroupInfo,
      Set<AggregateFunction> aggregateFunctions) {
    if (aggregateFunctions != null && edgeGroupInfo != null) {
      checkAggregationAndGroupingKeyIntersection(aggregateFunctions, edgeGroupInfo);
    }
    this.edgeGroupInfo = edgeGroupInfo;
    this.edgeAggregateFunctions = aggregateFunctions;
  }

  /**
   * Aggregates all edges in the provided window using the given information in the constructor.
   *
   * @param obsoleteStr   the key selector string, which is not used in this process
   * @param obsoleteContext context, which is not used in this process
   * @param tripletIt iterable of the triplets in this window
   * @param out the collector in which the aggregated triplet are collected
   */
  @Override
  public void process(String obsoleteStr, Context obsoleteContext,
      Iterable<Triplet> tripletIt,
      Collector<Triplet> out) {
    var aggregatedEdge = EdgeFactory.createEdge();

    Triplet lastEc = null;

    for (var triplet : tripletIt) {
      aggregatedEdge = (Edge) aggregateElement(aggregatedEdge, triplet.getEdge(),
          edgeAggregateFunctions
      );
      lastEc = triplet;
    }
    aggregatedEdge = (Edge) checkForMissingAggregationsAndApply(edgeAggregateFunctions,
        aggregatedEdge);
    Triplet aggregatedEContainer;

    // we have not set the grouped properties yet
    assert lastEc != null;
    aggregatedEdge = (Edge) setGroupedProperties(edgeGroupInfo,
        aggregatedEdge,
        lastEc.getEdge());
    var source = lastEc.getSourceVertex();
    var target = lastEc.getTargetVertex();
    aggregatedEdge.setSourceId(source.getId());
    aggregatedEdge.setTargetId(target.getId());
    aggregatedEContainer = new Triplet(aggregatedEdge, source,
        target);

    out.collect(aggregatedEContainer);
  }

}
