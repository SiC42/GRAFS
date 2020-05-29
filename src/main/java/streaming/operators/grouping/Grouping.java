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
import streaming.model.EdgeContainer;
import streaming.operators.OperatorI;
import streaming.operators.grouping.functions.AggregateMode;
import streaming.operators.grouping.functions.EdgeAggregation;
import streaming.operators.grouping.functions.EdgeKeySelector;
import streaming.operators.grouping.functions.GraphElementAggregationI;
import streaming.operators.grouping.functions.VertexAggregation;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

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
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return groupBy(stream);
  }

  public DataStream<EdgeContainer> groupBy(DataStream<EdgeContainer> es) {
    // TODO: Make sure that keys in egi has no intersection with keys in mapping

    ReduceFunction<Collection<Edge>> mergeCollection = (eColl1, eColl2) -> {
      eColl1.addAll(eColl2);
      return eColl1;
    };
    TriFunction<DataStream<EdgeContainer>, AggregateMode, GraphElementAggregationI, DataStream<EdgeContainer>> applyAggregation =
        (DataStream<EdgeContainer> stream,
            AggregateMode aggregateMode,
            GraphElementAggregationI aggregationFunction) ->
            stream
                .keyBy(new EdgeKeySelector(vertexEgi, edgeEgi, aggregateMode))
                .timeWindow(Time.milliseconds(10)) // TODO: Zeit nach außen tragen
                .apply(aggregationFunction);

    DataStream<EdgeContainer> aggregatedOnEdgeStream = applyAggregation.apply(
        es,
        AggregateMode.EDGE,
        new EdgeAggregation(vertexEgi, vertexAggregationFunctions, edgeEgi,
            edgeAggregationFunctions));

    // Enrich stream with reverse edges
    DataStream<EdgeContainer> expandedEdgeStream = aggregatedOnEdgeStream
        .flatMap(new FlatMapFunction<EdgeContainer, EdgeContainer>() {
          @Override
          public void flatMap(EdgeContainer value, Collector<EdgeContainer> out) {
            out.collect(value.createReverseEdgeContainer());
            out.collect(value);
          }
        });

    AggregateMode vertexAggregateMode = AggregateMode.SOURCE;
    DataStream<EdgeContainer> aggregatedOnSourceStream = applyAggregation.apply(
        expandedEdgeStream,
        vertexAggregateMode,
        new VertexAggregation(vertexEgi, vertexAggregationFunctions, vertexAggregateMode));

    vertexAggregateMode = AggregateMode.TARGET;
    DataStream<EdgeContainer> finalAggregatedStream = applyAggregation.apply(
        aggregatedOnSourceStream,
        vertexAggregateMode,
        new VertexAggregation(vertexEgi, vertexAggregationFunctions, vertexAggregateMode))
        .filter(e -> !e.getEdge().isReverse());

    return finalAggregatedStream;
  }

}
