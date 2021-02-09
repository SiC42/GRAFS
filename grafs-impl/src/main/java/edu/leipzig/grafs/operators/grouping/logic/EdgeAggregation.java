package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Provides the ability to aggregate on edges of the streams by providing the {@link
 * #process(String, Context, Iterable, Collector)} method.
 *
 * @param <W> the type of window to be used for the grouping
 */
public class EdgeAggregation<W extends Window> extends ElementAggregation<W> {


  private final GroupingInformation edgeGroupInfo;
  private final Set<AggregateFunction> edgeAggregateFunctions;
  private final GradoopId newGraphId;

  /**
   * Constructs the edge aggregation with the given information.
   *
   * @param edgeGroupInfo      grouping information used to determine which edges are in a group
   * @param aggregateFunctions aggregate functions that are used to calculate the aggregates and set
   *                           them in the aggregated edge
   * @param newGraphId         graph id used for the aggregated graph
   */
  public EdgeAggregation(GroupingInformation edgeGroupInfo,
      Set<AggregateFunction> aggregateFunctions, GradoopId newGraphId) {
    if (aggregateFunctions != null && edgeGroupInfo != null) {
      checkAggregationAndGroupingKeyIntersection(aggregateFunctions, edgeGroupInfo);
    }
    this.edgeGroupInfo = edgeGroupInfo;
    this.edgeAggregateFunctions = aggregateFunctions;
    this.newGraphId = newGraphId;
  }

  /**
   * Aggregates all edges in the provided window using the given information in the constructor.
   *
   * @param obsoleteStr     the key selector string, which is not used in this process
   * @param obsoleteContext context, which is not used in this process
   * @param tripletIt       iterable of the triplets in this window
   * @param out             the collector in which the aggregated triplet are collected
   */
  @Override
  public void process(String obsoleteStr, Context obsoleteContext,
      Iterable<Triplet<Vertex, Edge>> tripletIt,
      Collector<Triplet<Vertex, Edge>> out) {
    var aggregatedEdge = EdgeFactory.createEdge();

    Triplet<Vertex, Edge> lastTriplet = null;

    for (var triplet : tripletIt) {
      aggregatedEdge = (Edge) aggregateElement(aggregatedEdge, triplet.getEdge(),
          edgeAggregateFunctions
      );
      lastTriplet = triplet;
    }
    aggregatedEdge = (Edge) checkForMissingAggregationsAndApply(edgeAggregateFunctions,
        aggregatedEdge);
    Triplet<Vertex, Edge> aggregatedTriplet;

    // we have not set the grouped properties yet
    assert lastTriplet != null;
    aggregatedEdge = (Edge) setGroupedProperties(edgeGroupInfo,
        aggregatedEdge,
        lastTriplet.getEdge());
    aggregatedEdge.setGraphIds(GradoopIdSet.fromExisting(newGraphId));
    var source = lastTriplet.getSourceVertex();
    source.setGraphIds(GradoopIdSet.fromExisting(newGraphId));
    var target = lastTriplet.getTargetVertex();
    target.setGraphIds(GradoopIdSet.fromExisting(newGraphId));
    aggregatedEdge.setSourceId(source.getId());
    aggregatedEdge.setTargetId(target.getId());
    aggregatedTriplet = new Triplet<Vertex, Edge>(aggregatedEdge, source,
        target);

    out.collect(aggregatedTriplet);
  }

}
