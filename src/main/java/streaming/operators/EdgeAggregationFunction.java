package streaming.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.Vertex;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.ElementGroupingInformation;
import streaming.model.grouping.PropertiesAggregationFunction;

import java.util.Map;
import java.util.Set;

public class EdgeAggregationFunction implements FlatMapFunction<Set<Edge>, Edge> {


    private ElementGroupingInformation vertexEgi;
    private AggregationMapping vertexAggregationMapping;
    private ElementGroupingInformation edgeEgi;
    private AggregationMapping edgeAggregationMapping;

    public EdgeAggregationFunction(ElementGroupingInformation vertexEgi, AggregationMapping vertexAggregationMapping, ElementGroupingInformation edgeEgi, AggregationMapping edgeAggregationMapping) {
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
            aggregateGei(aggregatedSourceGei, e.getSource().getGei());
            aggregateGei(aggregatedTargetGei, e.getTarget().getGei());
            aggregateGei(aggregatedEdgeGei, e.getGei());
        }
        Vertex aggregatedSource = new Vertex(aggregatedSourceGei);
        Vertex aggregatedTarget = new Vertex(aggregatedTargetGei);
        Edge aggregatedEdge = new Edge(aggregatedSource, aggregatedTarget, aggregatedEdgeGei);
        out.collect(aggregatedEdge);

    }

    private void aggregateGei(GraphElementInformation aggregatedSourceGei, GraphElementInformation edgeGei) {
        for (Map.Entry<String, String> property : edgeGei.getProperties().entrySet()) {
            String key = property.getKey();
            if (vertexEgi.groupingKeys.contains(key)) {
                aggregatedSourceGei.addProperty(key, property.getValue());
            } else if (vertexAggregationMapping.contains(key)) {
                PropertiesAggregationFunction aF = vertexAggregationMapping.get(key);
                String prevValue = aggregatedSourceGei.containsProperty(key)
                        ? aggregatedSourceGei.getProperty(key)
                        : aF.getIdentity();
                String newValue = aF.apply(prevValue, edgeGei.getProperty(key));
                aggregatedSourceGei.addProperty(key, newValue);
            }
        }
    }
}
