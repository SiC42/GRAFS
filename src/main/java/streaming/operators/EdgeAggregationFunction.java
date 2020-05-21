package streaming.operators;

import java.util.Collection;
import org.apache.flink.util.Collector;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.Vertex;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.GroupingInformation;

public class EdgeAggregationFunction implements GraphElementAggregationFunctionI {


  private GroupingInformation vertexEgi;
  private AggregationMapping vertexAggregationMapping;
  private GroupingInformation edgeEgi;
  private AggregationMapping edgeAggregationMapping;

  public EdgeAggregationFunction(GroupingInformation vertexEgi,
      AggregationMapping vertexAggregationMapping, GroupingInformation edgeEgi,
      AggregationMapping edgeAggregationMapping) {
    this.vertexEgi = vertexEgi;
    this.vertexAggregationMapping = vertexAggregationMapping;
    this.edgeEgi = edgeEgi;
    this.edgeAggregationMapping = edgeAggregationMapping;
  }


  @Override
  public void flatMap(Collection<Edge> edgeSet, Collector<Edge> out) {
    GraphElementInformation aggregatedSourceGei = new GraphElementInformation();
    GraphElementInformation aggregatedTargetGei = new GraphElementInformation();
    GraphElementInformation aggregatedEdgeGei = new GraphElementInformation();

    for (Edge e : edgeSet) {
      aggregateGei(vertexAggregationMapping, vertexEgi, aggregatedSourceGei,
          e.getSource().getGei());
      aggregateGei(vertexAggregationMapping, vertexEgi, aggregatedTargetGei,
          e.getTarget().getGei());
      aggregateGei(edgeAggregationMapping, edgeEgi, aggregatedEdgeGei, e.getGei());
    }
    Vertex aggregatedSource = new Vertex(aggregatedSourceGei);
    Vertex aggregatedTarget = new Vertex(aggregatedTargetGei);
    Edge aggregatedEdge = new Edge(aggregatedSource, aggregatedTarget, aggregatedEdgeGei);
    out.collect(aggregatedEdge);

  }


}
