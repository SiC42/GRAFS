package edu.leipzig.grafs.operators.grouping;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.logic.EdgeAggregation;
import edu.leipzig.grafs.operators.grouping.logic.EdgeKeySelector;
import edu.leipzig.grafs.operators.grouping.logic.VertexAggregation;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

public class Grouping<W extends Window> implements GraphToGraphOperatorI {

  private final GroupingInformation vertexGi;
  private final Set<AggregateFunction> vertexAggregateFunctions;
  private final GroupingInformation edgeGi;
  private final Set<AggregateFunction> edgeAggregateFunctions;

  private final WindowAssigner<Object, W> window;
  private final Trigger<EdgeContainer, W> trigger;

  public Grouping(GroupingInformation vertexGi, Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    this.vertexGi = vertexGi;
    this.vertexAggregateFunctions = vertexAggregateFunctions;
    this.edgeGi = edgeGi;
    this.edgeAggregateFunctions = edgeAggregateFunctions;
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
    var expandedStream = createStreamWithReverseEdges(stream);

    var aggregatedOnSourceStream = aggregateOnVertex(expandedStream, AggregateMode.SOURCE);
    var aggregatedOnVertexStream = aggregateOnVertex(aggregatedOnSourceStream,
        AggregateMode.TARGET);
    var reducedStream = aggregatedOnVertexStream.filter(ec -> !ec.getEdge().isReverse());
    var aggregatedOnEdgeStream = aggregateOnEdge(reducedStream);
    var graphId = GradoopId.get();
    return aggregatedOnEdgeStream.map(ec -> {
      ec.addGraphId(graphId);
      return ec;
    });
  }

  private SingleOutputStreamOperator<EdgeContainer> createStreamWithReverseEdges(
      DataStream<EdgeContainer> stream) {
    return stream
        .flatMap(new FlatMapFunction<EdgeContainer, EdgeContainer>() {
          @Override
          public void flatMap(EdgeContainer value, Collector<EdgeContainer> out) {
            out.collect(value.createReverseEdgeContainer());
            out.collect(value);
          }
        });
  }

  private DataStream<EdgeContainer> aggregateOnVertex(DataStream<EdgeContainer> stream,
      AggregateMode mode) {
    var windowedStream = createKeyedWindowedStream(stream, mode);
    return windowedStream.process(
        new VertexAggregation<>(vertexGi, vertexAggregateFunctions, mode));
  }

  private DataStream<EdgeContainer> aggregateOnEdge(DataStream<EdgeContainer> stream) {
    var windowedStream = createKeyedWindowedStream(stream, AggregateMode.EDGE);
    return windowedStream.process(new EdgeAggregation<W>(edgeGi, edgeAggregateFunctions));
  }

  private WindowedStream<EdgeContainer, String, W> createKeyedWindowedStream(
      DataStream<EdgeContainer> es, AggregateMode mode) {
    var windowedStream = es
        .keyBy(new EdgeKeySelector(vertexGi, edgeGi, mode))
        .window(window);
    if (trigger != null) {
      windowedStream = windowedStream.trigger(trigger);
    }
    return windowedStream;
  }

  public static final class GroupingBuilder {

    private final GroupingInformation vertexGi;
    private final Set<AggregateFunction> vertexAggFunctions;

    private final GroupingInformation edgeGi;
    private final Set<AggregateFunction> aggregateFunctions;

    public GroupingBuilder() {
      vertexGi = new GroupingInformation();
      vertexAggFunctions = new HashSet<>();
      edgeGi = new GroupingInformation();
      aggregateFunctions = new HashSet<>();
    }

    public GroupingBuilder addVertexGroupingKey(String vertexGroupingKey) {
      vertexGi.addKey(vertexGroupingKey);
      return this;
    }

    public GroupingBuilder addVertexGroupingKeys(Set<String> vertexGroupingKeys) {
      vertexGi.addKeys(vertexGroupingKeys);
      return this;
    }

    public GroupingBuilder addEdgeGroupingKey(String edgeGroupingKey) {
      edgeGi.addKey(edgeGroupingKey);
      return this;
    }

    public GroupingBuilder addEdgeGroupingKeys(Set<String> edgeGroupingKeys) {
      edgeGi.addKeys(edgeGroupingKeys);
      return this;
    }

    public <W extends Window> Grouping<W> buildWithWindow(WindowAssigner<Object, W> window) {
      return new Grouping<>(vertexGi, vertexAggFunctions, edgeGi, aggregateFunctions, window, null);
    }

    public <W extends Window> Grouping<W> buildWithWindowAndTrigger(
        WindowAssigner<Object, W> window,
        Trigger<EdgeContainer, W> trigger) {
      return new Grouping<>(vertexGi, vertexAggFunctions, edgeGi, aggregateFunctions, window,
          trigger);
    }

    /**
     * Define, if the vertex label shall be used for grouping vertices.
     *
     * @param useVertexLabel true, iff vertex label shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useVertexLabel(boolean useVertexLabel) {
      vertexGi.useLabel(useVertexLabel);
      return this;
    }

    /**
     * Define, if the edge label shall be used for grouping edges.
     *
     * @param useEdgeLabel true, iff edge label shall be used for grouping
     * @return this builder
     */
    public GroupingBuilder useEdgeLabel(boolean useEdgeLabel) {
      edgeGi.useLabel(useEdgeLabel);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all vertices represented by a single super
     * vertex.
     *
     * @param aggregateFunction vertex aggregate mapping
     * @return this builder
     */
    public GroupingBuilder addVertexAggregateFunction(AggregateFunction aggregateFunction) {
      Objects.requireNonNull(aggregateFunction, "Aggregate function must not be null");
      vertexAggFunctions.add(aggregateFunction);
      return this;
    }

    /**
     * Add an aggregate function which is applied on all edges represented by a single super edge.
     *
     * @param eFunctions edge aggregate mapping
     * @return this builder
     */
    public GroupingBuilder addEdgeAggregateFunction(AggregateFunction eFunctions) {
      Objects.requireNonNull(eFunctions, "Aggregate function must not be null");
      aggregateFunctions.add(eFunctions);
      return this;
    }

  }
}
