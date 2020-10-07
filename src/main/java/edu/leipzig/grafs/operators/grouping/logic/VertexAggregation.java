package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Provides the ability to aggregate on vertices of the streams by providing the {@link
 * #process(String, Context, Iterable, Collector)} method.
 *
 * @param <W> the type of window to be used for the grouping
 */
public class VertexAggregation<W extends Window> extends VertexAggregationProcess<W> {

  private final GroupingInformation vertexGroupInfo;
  private final Set<AggregateFunction> aggregateFunctions;
  private final AggregateMode aggregateMode;

  /**
   * Constructs the vertex aggregation with the given information.
   *
   * @param vertexGroupInfo    grouping information used to determine which vertex are in a group
   * @param aggregateFunctions aggregate functions that are used to calculate the aggregates and set
   *                           them in the aggregated vertex
   * @param aggregateMode      determines if the source or target vertex of the edge stream should
   *                           be aggregated
   */
  public VertexAggregation(GroupingInformation vertexGroupInfo,
      Set<AggregateFunction> aggregateFunctions, AggregateMode aggregateMode) {
    checkAggregationAndGroupingKeyIntersection(aggregateFunctions, vertexGroupInfo);
    this.vertexGroupInfo = vertexGroupInfo;
    this.aggregateFunctions = aggregateFunctions;
    this.aggregateMode = aggregateMode;
  }

  /**
   * Aggregates all vertices in the provided window using the given information in the constructor.
   *
   * @param obsoleteStr     the key selector string, which is not used in this process
   * @param obsoleteContext context, which is not used in this process
   * @param ecIterable      iterable of the edge containers in this window
   * @param out             the collector in which the aggregated edge container are collected
   */
  @Override
  public void process(String obsoleteStr, Context obsoleteContext,
      Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> out) {
    var aggregatedVertex = new AggregatedVertex();

    // determine the aggregated vertice
    var isInitialAggregation = true;
    for (EdgeContainer ec : ecIterable) {
      Vertex curVertex;
      if (aggregateMode.equals(AggregateMode.SOURCE)) {
        curVertex = ec.getSourceVertex();
      } else {
        curVertex = ec.getTargetVertex();
      }
      if (isInitialAggregation) {
        isInitialAggregation = false;
        aggregatedVertex = (AggregatedVertex) setGroupedProperties(vertexGroupInfo,
            aggregatedVertex, curVertex);
      }
      aggregatedVertex = aggregateVertex(aggregatedVertex, curVertex, aggregateFunctions);
    }
    aggregatedVertex = (AggregatedVertex) checkForMissingAggregationsAndApply(aggregateFunctions,
        aggregatedVertex);

    // build new edge containers using the aggregated vertice
    for (EdgeContainer ec : ecIterable) {
      if (ec.getEdge().isReverse()) {
        out.collect(ec); // No need to aggregate for reverse edges
        continue;
      }
      Vertex finalVertex = VertexFactory.createVertex(aggregatedVertex);
      EdgeContainer aggregatedEC;
      var edge = ec.getEdge();
      if (aggregateMode.equals(AggregateMode.SOURCE)) {
        var newEdge = EdgeFactory.createEdge(edge.getLabel(),
            finalVertex.getId(),
            ec.getTargetVertex().getId(),
            edge.getProperties());
        aggregatedEC = new EdgeContainer(newEdge, finalVertex, ec.getTargetVertex());
      } else { // TARGET-mode
        var newEdge = EdgeFactory.createEdge(edge.getLabel(),
            ec.getSourceVertex().getId(),
            finalVertex.getId(),
            edge.getProperties());
        aggregatedEC = new EdgeContainer(newEdge, ec.getSourceVertex(), finalVertex);
      }
      out.collect(aggregatedEC);

    }
  }

}
