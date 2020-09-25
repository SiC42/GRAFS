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

public class VertexAggregation<W extends Window> extends VertexAggregationProcess<W> {

  private final GroupingInformation vertexGroupInfo;
  private final Set<AggregateFunction> aggregateFunctions;
  private final AggregateMode aggregateMode;

  public VertexAggregation(GroupingInformation vertexGroupInfo,
      Set<AggregateFunction> aggregateFunctions, AggregateMode aggregateMode) {
    checkAggregationAndGroupingKeyIntersection(aggregateFunctions, vertexGroupInfo);
    this.vertexGroupInfo = vertexGroupInfo;
    this.aggregateFunctions = aggregateFunctions;
    this.aggregateMode = aggregateMode;
  }

  @Override
  public void process(String s, Context context, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> out) {
    var aggregatedVertex = new AggregatedVertex();

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
      aggregatedVertex = aggregateVertex(aggregateFunctions, aggregatedVertex, curVertex);
    }
    for (EdgeContainer ec : ecIterable) {
      if (ec.getEdge().isReverse()) {
        out.collect(ec); // No need to aggregate for reverse edges
      } else {
        Vertex finalVertex = VertexFactory.createVertex(aggregatedVertex);
        EdgeContainer aggregatedEdge;
        var edge = ec.getEdge();
        if (aggregateMode.equals(AggregateMode.SOURCE)) {
          var newEdge = EdgeFactory.createEdge(edge.getLabel(),
              finalVertex.getId(),
              ec.getTargetVertex().getId(),
              edge.getProperties());
          aggregatedEdge = new EdgeContainer(newEdge, finalVertex, ec.getTargetVertex());
        } else { // TARGET-mode
          var newEdge = EdgeFactory.createEdge(edge.getLabel(),
              ec.getSourceVertex().getId(),
              finalVertex.getId(),
              edge.getProperties());
          aggregatedEdge = new EdgeContainer(newEdge, ec.getSourceVertex(), finalVertex);
        }
        out.collect(aggregatedEdge);
      }
    }
  }

}
