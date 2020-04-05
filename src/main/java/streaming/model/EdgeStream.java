package streaming.model;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.TriFunction;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.ElementGroupingInformation;
import streaming.operators.AggregateMode;
import streaming.operators.EdgeAggregationFunction;
import streaming.operators.EdgeKeySelector;
import streaming.operators.GraphElementAggregationFunctionI;
import streaming.operators.VertexAggregationFunction;

public class EdgeStream {


  private final MapFunction<Edge, Collection<Edge>> edgeToSingleSetFunction = new MapFunction<Edge, Collection<Edge>>() {
    @Override
    public Collection<Edge> map(Edge edge) {
      Collection<Edge> singleSet = new HashSet<>();
      singleSet.add(edge);
      return singleSet;
    }
  };
  private final ReduceFunction<Collection<Edge>> mergeCollection = (eColl1, eColl2) -> {
    eColl1.addAll(eColl2);
    return eColl1;
  };
  private DataStream<Edge> edgeStream;

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

  public EdgeStream vertexInducedSubgraph(
      FilterFunction<GraphElementInformation> vertexGeiPredicate) {
    FilterFunction<Edge> edgePredicate = edge ->
        vertexGeiPredicate.filter(edge.getSource().getGei()) && vertexGeiPredicate
            .filter(edge.getTarget().getGei());

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

    TriFunction<DataStream<Edge>, AggregateMode, GraphElementAggregationFunctionI, DataStream<Edge>> applyAggregation =
        (DataStream<Edge> stream,
            AggregateMode aggregateMode,
            GraphElementAggregationFunctionI aggregationFunction) ->
            stream
                .map(edgeToSingleSetFunction)
                .keyBy(new EdgeKeySelector(vertexEgi, edgeEgi, aggregateMode))
                .timeWindow(Time.milliseconds(10)) // TODO: Zeit nach außen tragen
                .reduce(mergeCollection)
                .flatMap(aggregationFunction);

    DataStream<Edge> aggregatedOnEdgeStream = applyAggregation.apply(
        edgeStream,
        AggregateMode.EDGE,
        new EdgeAggregationFunction(vertexEgi, vertexAggregationFunctions, edgeEgi,
            edgeAggregationFunctions));

    // Enrich stream with reverse edges
    DataStream<Edge> expandedEdgeStream = aggregatedOnEdgeStream
        .flatMap(new FlatMapFunction<Edge, Edge>() {
          @Override
          public void flatMap(Edge value, Collector<Edge> out) {
            out.collect(value.createReverseEdge());
            out.collect(value);
          }
        });

    AggregateMode vertexAggregateMode = AggregateMode.SOURCE;
    DataStream<Edge> aggregatedOnSourceStream = applyAggregation.apply(
        expandedEdgeStream,
        vertexAggregateMode,
        new VertexAggregationFunction(vertexEgi, vertexAggregationFunctions, vertexAggregateMode));

    vertexAggregateMode = AggregateMode.TARGET;
    DataStream<Edge> finalAggregatedStream = applyAggregation.apply(
        aggregatedOnSourceStream,
        vertexAggregateMode,
        new VertexAggregationFunction(vertexEgi, vertexAggregationFunctions, vertexAggregateMode))
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