package streaming.operators;

import java.util.Set;
import org.apache.flink.util.Collector;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.Vertex;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.ElementGroupingInformation;

public class EdgeAggregationFunction implements GraphElementAggregationFunctionI {


  private ElementGroupingInformation vertexEgi;
  private AggregationMapping vertexAggregationMapping;
  private ElementGroupingInformation edgeEgi;
  private AggregationMapping edgeAggregationMapping;

  public EdgeAggregationFunction(ElementGroupingInformation vertexEgi,
      AggregationMapping vertexAggregationMapping, ElementGroupingInformation edgeEgi,
      AggregationMapping edgeAggregationMapping) {
    this.vertexEgi = vertexEgi;
    this.vertexAggregationMapping = vertexAggregationMapping;
    this.edgeEgi = edgeEgi;
    this.edgeAggregationMapping = edgeAggregationMapping;
  }


  @Override
  public void flatMap(Set<Edge> edgeSet, Collector<Edge> out) {
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
