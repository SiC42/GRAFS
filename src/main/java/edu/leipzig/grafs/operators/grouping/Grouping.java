package edu.leipzig.grafs.operators.grouping;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.OperatorI;
import edu.leipzig.grafs.operators.grouping.logic.EdgeAggregation;
import edu.leipzig.grafs.operators.grouping.logic.EdgeKeySelector;
import edu.leipzig.grafs.operators.grouping.logic.GraphElementAggregationProcess;
import edu.leipzig.grafs.operators.grouping.logic.VertexAggregation;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.TriFunction;

public class Grouping implements OperatorI {

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
    TriFunction<DataStream<EdgeContainer>, AggregateMode, GraphElementAggregationProcess, DataStream<EdgeContainer>> applyAggregation =
        (DataStream<EdgeContainer> stream,
            AggregateMode aggregateMode,
            GraphElementAggregationProcess aggregationFunction) ->
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

    return applyAggregation.apply(
        aggregatedOnSourceStream,
        vertexAggregateMode,
        new VertexAggregation(vertexEgi, vertexAggregationFunctions, vertexAggregateMode))
        .filter(e -> !e.getEdge().isReverse());
  }

}
