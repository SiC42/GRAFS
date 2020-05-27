package streaming.operators.grouping;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.TriFunction;
import streaming.model.Edge;
import streaming.model.EdgeStream;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;
import streaming.operators.grouping.functions.AggregateMode;
import streaming.operators.grouping.functions.EdgeAggregation;
import streaming.operators.grouping.functions.EdgeKeySelector;
import streaming.operators.grouping.functions.GraphElementAggregationI;
import streaming.operators.OperatorI;
import streaming.operators.grouping.functions.VertexAggregation;

public class Grouping implements OperatorI {

  private final MapFunction<Edge, Collection<Edge>> edgeToSingleSetFunction = new MapFunction<Edge, Collection<Edge>>() {
    @Override
    public Collection<Edge> map(Edge edge) {
      Collection<Edge> singleSet = new ArrayList<>();
      singleSet.add(edge);
      return singleSet;
    }
  };

  private final GroupingInformation vertexEgi;
  private final AggregationMapping vertexAggregationFunctions;
  private final GroupingInformation edgeEgi;
  private final AggregationMapping edgeAggregationFunctions;

  public Grouping(final GroupingInformation vertexEgi,
      final AggregationMapping vertexAggregationFunctions,
      final GroupingInformation edgeEgi,
      final AggregationMapping edgeAggregationFunctions) {
    this.vertexEgi = vertexEgi;
    this.vertexAggregationFunctions = vertexAggregationFunctions;
    this.edgeEgi = edgeEgi;
    this.edgeAggregationFunctions = edgeAggregationFunctions;
  }

  @Override
  public DataStream<Edge> execute(DataStream<Edge> stream) {
    return groupBy(stream);
  }

  public DataStream<Edge> groupBy(DataStream<Edge> es) {
    // TODO: Make sure that keys in egi has no intersection with keys in mapping

    ReduceFunction<Collection<Edge>> mergeCollection = (eColl1, eColl2) -> {
      eColl1.addAll(eColl2);
      return eColl1;
    };
    TriFunction<DataStream<Edge>, AggregateMode, GraphElementAggregationI, DataStream<Edge>> applyAggregation =
        (DataStream<Edge> stream,
            AggregateMode aggregateMode,
            GraphElementAggregationI aggregationFunction) ->
            stream
                .map(edgeToSingleSetFunction)
                .keyBy(new EdgeKeySelector(vertexEgi, edgeEgi, aggregateMode))
                .timeWindow(Time.milliseconds(10)) // TODO: Zeit nach außen tragen
                .reduce(mergeCollection)
                .flatMap(aggregationFunction);

    DataStream<Edge> aggregatedOnEdgeStream = applyAggregation.apply(
        es,
        AggregateMode.EDGE,
        new EdgeAggregation(vertexEgi, vertexAggregationFunctions, edgeEgi,
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
        new VertexAggregation(vertexEgi, vertexAggregationFunctions, vertexAggregateMode));

    vertexAggregateMode = AggregateMode.TARGET;
    DataStream<Edge> finalAggregatedStream = applyAggregation.apply(
        aggregatedOnSourceStream,
        vertexAggregateMode,
        new VertexAggregation(vertexEgi, vertexAggregationFunctions, vertexAggregateMode))
        .filter(e -> !e.isReverse());

    return finalAggregatedStream;
  }
}
