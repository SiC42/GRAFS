package edu.leipzig.grafs.operators.grouping;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.OperatorI;
import edu.leipzig.grafs.operators.grouping.logic.EdgeAggregation;
import edu.leipzig.grafs.operators.grouping.logic.EdgeKeySelector;
import edu.leipzig.grafs.operators.grouping.logic.VertexAggregation;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class Grouping<W extends Window> implements OperatorI {

  private final GroupingInformation vertexGi;
  private final AggregationMapping vertexAggregationFunctions;
  private final GroupingInformation edgeGi;
  private final AggregationMapping edgeAggregationFunctions;

  private final WindowAssigner<Object, W> window;
  private final Trigger<EdgeContainer, W> trigger;

  public Grouping(GroupingInformation vertexGi, AggregationMapping vertexAggMap,
      GroupingInformation edgeGi, AggregationMapping edgeAggMap, WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    this.vertexGi = vertexGi;
    this.vertexAggregationFunctions = vertexAggMap;
    this.edgeGi = edgeGi;
    this.edgeAggregationFunctions = edgeAggMap;
    this.window = window;
    this.trigger = trigger;
  }

  public static GroupingBuilder createGrouping() {
    return new GroupingBuilder();
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return groupBy(stream);
  }

  public DataStream<EdgeContainer> groupBy(DataStream<EdgeContainer> stream) {

    // Enrich stream with reverse edges
    var expandedStream = stream
        .flatMap(new FlatMapFunction<EdgeContainer, EdgeContainer>() {
          @Override
          public void flatMap(EdgeContainer value, Collector<EdgeContainer> out) {
            out.collect(value.createReverseEdgeContainer());
            out.collect(value);
          }
        });

    var aggregatedOnSourceStream = aggregateOnVertex(expandedStream, AggregateMode.SOURCE);
    var aggregateOnVertexStream = aggregateOnVertex(expandedStream, AggregateMode.TARGET);

    return aggregateOnEdge(aggregateOnVertexStream);
  }

  private DataStream<EdgeContainer> aggregateOnVertex(DataStream<EdgeContainer> stream,
      AggregateMode mode) {
    var windowedStream = createKeyedWindowedStream(stream, mode);
    return windowedStream.process(
        new VertexAggregation<W>(vertexGi, vertexAggregationFunctions, mode));
  }

  private DataStream<EdgeContainer> aggregateOnEdge(DataStream<EdgeContainer> stream) {
    var windowedStream = createKeyedWindowedStream(stream, AggregateMode.EDGE);
    return windowedStream.process(new EdgeAggregation<W>(edgeGi, edgeAggregationFunctions));
  }

  private WindowedStream<EdgeContainer, String, W> createKeyedWindowedStream(
      DataStream<EdgeContainer> es, AggregateMode edge) {
    var windowedStream = es
        .keyBy(new EdgeKeySelector(vertexGi, edgeGi, AggregateMode.EDGE))
        .window(window);
    if (trigger != null) {
      windowedStream = windowedStream.trigger(trigger);
    }
    return windowedStream;
  }

  public static class GroupingBuilder {

    private GroupingInformation vertexGi = null;
    private AggregationMapping vertexAggMap = null;

    private GroupingInformation edgeGi = null;
    private AggregationMapping edgeAggMap = null;

    public GroupingBuilder withVertexGrouping(GroupingInformation vertexGi,
        AggregationMapping vertexAggMap) {
      this.vertexGi = vertexGi;
      this.vertexAggMap = vertexAggMap;
      return this;
    }

    public GroupingBuilder withEdgeGrouping(GroupingInformation edgeGi,
        AggregationMapping edgeAggMap) {
      this.edgeGi = edgeGi;
      this.edgeAggMap = edgeAggMap;
      return this;
    }

    public <W extends Window> Grouping<W> buildWithWindow(WindowAssigner<Object, W> window) {
      return new Grouping<>(vertexGi, vertexAggMap, edgeGi, edgeAggMap, window, null);
    }

    public <W extends Window> Grouping<W> buildWithWindowAndTrigger(
        WindowAssigner<Object, W> window,
        Trigger<EdgeContainer, W> trigger) {
      return new Grouping<>(vertexGi, vertexAggMap, edgeGi, edgeAggMap, window, trigger);
    }

  }
}
