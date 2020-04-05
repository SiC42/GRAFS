package streaming.operators;

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

public class EdgeAggregationFunction implements FlatMapFunction<Set<Edge>, Edge>, Serializable {

    private ElementGroupingInformation egi;
    private AggregationMapping aggregationMapping;
    private AggregateMode aggregateMode;

    public EdgeAggregationFunction(ElementGroupingInformation egi, AggregationMapping aggregationMapping, AggregateMode aggregateMode) {
        this.egi = egi;
        this.aggregationMapping = aggregationMapping;
        this.aggregateMode = aggregateMode;
    }


    @Override
    public void flatMap(Set<Edge> edgeSet, Collector<Edge> out) {
        GraphElementInformation aggregatedGei = new GraphElementInformation();
        for (Edge e : edgeSet) {
            GraphElementInformation sourceGei = aggregateMode.equals(AggregateMode.SOURCE) ? e.getSource().getGei() : e.getTarget().getGei();
            for (Map.Entry<String, String> property : sourceGei.getProperties().entrySet()) {
                String key = property.getKey();
                if (egi.groupingKeys.contains(key)) {
                    aggregatedGei.addProperty(key, property.getValue());
                } else if(aggregationMapping.contains(key)) {
                    PropertiesAggregationFunction aF = aggregationMapping.get(key);
                    String prevValue = aggregatedGei.containsProperty(key)
                            ? aggregatedGei.getProperty(key)
                            : aF.getIdentity();
                    String newValue = aF.apply(prevValue, sourceGei.getProperty(key));
                    aggregatedGei.addProperty(key, newValue);
                }
            }
        }
        for (Edge e : edgeSet) {
            if (!e.isReverse()) {
                Vertex aggregatedVertex = new Vertex(aggregatedGei);
                Edge aggregatedEdge = null;
                switch (aggregateMode) {
                    case SOURCE: {
                        aggregatedEdge = new Edge(aggregatedVertex, e.getTarget(), e.getGei());
                        break;
                    }
                    case TARGET: {
                        aggregatedEdge = new Edge(e.getSource(), aggregatedVertex, e.getGei());
                        break;
                    }
                    case EDGE: {
                        break;
                    }
                }
                out.collect(aggregatedEdge);
            } else {
                out.collect(e);
            }
        }
    }
}
