package streaming.model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.ElementGroupingInformation;
import streaming.operators.AggregateMode;
import streaming.operators.EdgeAggregationFunction;
import streaming.operators.EdgeKeySelector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class EdgeStream {


    private DataStream<Edge> edgeStream;

    private final MapFunction<Edge, Set<Edge>> edgeToSingleSetFunction = new MapFunction<Edge, Set<Edge>>() {
        @Override
        public Set<Edge> map(Edge edge) {
            Set<Edge> singleSet = new HashSet<>();
            singleSet.add(edge);
            return singleSet;
        }
    };

    private final ReduceFunction<Set<Edge>> mergeSets = (Set<Edge> eS1, Set<Edge> eS2) -> {
        eS1.addAll(eS2);
        return eS1;
    };

    public EdgeStream(DataStream<Edge> edgeStream) {
        this.edgeStream = edgeStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Edge>() {
                    @Override
                    public long extractAscendingTimestamp(Edge edge) {
                        return 0;
                    }
                }
        );
    }

    public EdgeStream vertexInducedSubgraph(FilterFunction<GraphElementInformation> vertexGeiPredicate) {
        FilterFunction<Edge> edgePredicate = edge ->
                vertexGeiPredicate.filter(edge.getSource().getGei()) && vertexGeiPredicate.filter(edge.getTarget().getGei());

        return subgraph(edgePredicate);
    }

    public EdgeStream edgeInducedSubgraph(FilterFunction<GraphElementInformation> edgeGeiPredicate) {
        FilterFunction<Edge> edgePredicate = edge -> edgeGeiPredicate.filter(edge.getGei());
        return subgraph(edgePredicate);
    }

    public EdgeStream groupBy(ElementGroupingInformation vertexEgi,
                              AggregationMapping vertexAggregationFunctions,
                              ElementGroupingInformation edgeEgi, AggregationMapping edgeAggregationFunctions) {
        // TODO: Make sure that keys in egi has no intersection with keys in mapping

        DataStream<Edge> aggregatedOnEdgeStream = edgeStream
                .map(edgeToSingleSetFunction)
                .keyBy(new EdgeKeySelector(vertexEgi, edgeEgi, AggregateMode.EDGE))
                .timeWindow(Time.milliseconds(10)) // TODO: Zeit nach außen tragen
                .reduce(mergeSets)
                .flatMap(new VertexAggregationFunction(vertexEgi, vertexAggregationFunctions,
                        AggregateMode.EDGE));

        DataStream<Edge> expandedEdgeStream = aggregatedOnEdgeStream
                .flatMap(new FlatMapFunction<Edge, Edge>() {
                    @Override
                    public void flatMap(Edge value, Collector<Edge> out) {
                        out.collect(value.createReverseEdge());
                        out.collect(value);
                    }
                });

        DataStream<Edge> aggregatedOnSourceStream = expandedEdgeStream
                .map(edgeToSingleSetFunction)
                .keyBy(new EdgeKeySelector(vertexEgi, edgeEgi, AggregateMode.SOURCE))
                .timeWindow(Time.milliseconds(10)) // TODO: Zeit nach außen tragen
                .reduce(mergeSets)
                .flatMap(new VertexAggregationFunction(vertexEgi, vertexAggregationFunctions, AggregateMode.SOURCE));

        DataStream<Edge> finalAggregatedStream = aggregatedOnSourceStream
                .map(edgeToSingleSetFunction)
                .keyBy(new EdgeKeySelector(vertexEgi, edgeEgi, AggregateMode.TARGET))
                .timeWindow(Time.milliseconds(10)) // TODO: Zeit nach außen tragen
                .reduce(mergeSets)
                .flatMap(new VertexAggregationFunction(vertexEgi, vertexAggregationFunctions,
                        AggregateMode.TARGET))
                .filter(e -> !e.isReverse());

        return new EdgeStream(finalAggregatedStream);
    }


    public EdgeStream subgraph(FilterFunction<Edge> vertexPredicate) {
        DataStream<Edge> filteredStream = edgeStream.filter(vertexPredicate);
        return new EdgeStream(filteredStream);
    }

    public EdgeStream transform(MapFunction<Edge, Edge> mapper) {
        DataStream<Edge> filteredStream = edgeStream.map(mapper);
        return new EdgeStream(filteredStream);
    }

    public EdgeStream transformVertices(MapFunction<Vertex, Vertex> mapper) {
        MapFunction<Edge, Edge> transformVerticesFunction =
                edge -> {
                    Vertex from = mapper.map(edge.getSource());
                    Vertex to = mapper.map(edge.getTarget());
                    return new Edge(from, to, edge.getGei());
                };
        return transform(transformVerticesFunction);
    }

    public void print() {
        edgeStream.print();
    }

    public Iterator<Edge> collect() throws IOException {
        return DataStreamUtils.collect(edgeStream);
    }
}