package streaming.operators;

import com.google.common.base.Function;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.Vertex;
import streaming.model.grouping.PropertiesAggregationFunction;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.ElementGroupingInformation;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

public class EdgeAggregationFunction implements FlatMapFunction<Set<Edge>, Edge>, Serializable {

    private ElementGroupingInformation vertexEgi;
    private AggregationMapping aggregationMapping;
    private AggregateMode aggregateMode;

    public EdgeAggregationFunction(ElementGroupingInformation vertexEgi, AggregationMapping aggregationMapping, AggregateMode aggregateMode) {
        this.vertexEgi = vertexEgi;
        this.aggregationMapping = aggregationMapping;
        this.aggregateMode = aggregateMode;
    }

    @Override
    public void flatMap(Set<Edge> edgeSet, Collector<Edge> out) {
        switch (aggregateMode) {
            case SOURCE:
            case TARGET:
                flatMapForVertexAggregation(edgeSet, out);
                break;
            case EDGE:
                flatMapForEdgeAggregation(edgeSet, out);
        }

    }


    public void flatMapForVertexAggregation(Set<Edge> edgeSet, Collector<Edge> out) {
        GraphElementInformation aggregatedGei = new GraphElementInformation();
        Function<Edge, Vertex> getVertex = aggregateMode.equals(AggregateMode.SOURCE) ? Edge::getSource : Edge::getTarget;
        for (Edge e : edgeSet) {
            GraphElementInformation vertexGei = getVertex.apply(e).getGei();
            aggregateGei(aggregatedGei, vertexGei);
        }
        BiFunction<Vertex, Edge, Edge> generateNewEdge = aggregateMode.equals(AggregateMode.SOURCE)
                ? (v, e) -> new Edge(v, e.getTarget(), e.getGei())
                : (v, e) -> new Edge(e.getSource(), v, e.getGei());
        for (Edge e : edgeSet) {
            if (!e.isReverse()) {
                Vertex aggregatedVertex = new Vertex(aggregatedGei);
                Edge aggregatedEdge = generateNewEdge.apply(aggregatedVertex, e);
                out.collect(aggregatedEdge);
            } else {
                out.collect(e);
            }
        }
    }


    private void flatMapForEdgeAggregation(Set<Edge> edgeSet, Collector<Edge> out) {
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
            } else if (aggregationMapping.contains(key)) {
                PropertiesAggregationFunction aF = aggregationMapping.get(key);
                String prevValue = aggregatedSourceGei.containsProperty(key)
                        ? aggregatedSourceGei.getProperty(key)
                        : aF.getIdentity();
                String newValue = aF.apply(prevValue, edgeGei.getProperty(key));
                aggregatedSourceGei.addProperty(key, newValue);
            }
        }
    }
}
