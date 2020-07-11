package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.AggregatedVertex;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.function.BiFunction;
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

    boolean initialAggregation = true;

    for (EdgeContainer ec : ecIterable) {
      Vertex curVertex;
      if (aggregateMode.equals(AggregateMode.SOURCE)) {
        curVertex = ec.getSourceVertex();
      } else {
        curVertex = ec.getTargetVertex();
      }
      if (initialAggregation) {
        initialAggregation = false;
        aggregatedVertex = (AggregatedVertex) setGroupedProperties(vertexGroupInfo,
            aggregatedVertex, curVertex);
      }
      aggregatedVertex = aggregateVertex(aggregationMapping, aggregatedVertex, curVertex);
    }
    BiFunction<Vertex, EdgeContainer, EdgeContainer> generateUpdatedECFunction =
        aggregateMode.equals(AggregateMode.SOURCE)
            ? (v, ec) -> new EdgeContainer(ec.getEdge(), v, ec.getTargetVertex())
            : (v, ec) -> new EdgeContainer(ec.getEdge(), ec.getSourceVertex(), v);
    for (EdgeContainer ec : ecIterable) {
      if (!ec.getEdge().isReverse()) {
        Vertex finalVertex = new VertexFactory().createVertex(aggregatedVertex);
        EdgeContainer aggregatedEdge = generateUpdatedECFunction.apply(finalVertex, ec);
        out.collect(aggregatedEdge);
      } else {
        out.collect(ec);
      }
    }
  }

}
