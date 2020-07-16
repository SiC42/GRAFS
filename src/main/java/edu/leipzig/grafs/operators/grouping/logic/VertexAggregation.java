package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class VertexAggregation<W extends Window> extends VertexAggregationProcess<W> {

  private final GroupingInformation vertexGroupInfo;
  private final AggregationMapping aggregationMapping;
  private final AggregateMode aggregateMode;

  public VertexAggregation(GroupingInformation vertexGroupInfo,
      AggregationMapping aggregationMapping, AggregateMode aggregateMode) {
    checkAggregationAndGroupingKeyIntersection(aggregationMapping, vertexGroupInfo);
    this.vertexGroupInfo = vertexGroupInfo;
    this.aggregationMapping = aggregationMapping;
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
      aggregatedVertex = aggregateVertex(aggregationMapping, aggregatedVertex, curVertex);
    }
    for (EdgeContainer ec : ecIterable) {
      if (!ec.getEdge().isReverse()) {
        Vertex finalVertex = new VertexFactory().createVertex(aggregatedVertex);
        EdgeContainer aggregatedEdge;
        if (aggregateMode.equals(AggregateMode.SOURCE)) {
          aggregatedEdge = new EdgeContainer(ec.getEdge(), finalVertex, ec.getTargetVertex());
        } else { // TARGET-mode
          aggregatedEdge = new EdgeContainer(ec.getEdge(), ec.getSourceVertex(), finalVertex);
        }
        out.collect(aggregatedEdge);
      } else {
        out.collect(ec);
      }
    }
  }

}
