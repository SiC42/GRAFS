package edu.leipzig.grafs.operators.grouping;

import com.google.common.annotations.Beta;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.logic.EdgeAggregation;
import edu.leipzig.grafs.operators.grouping.logic.TripletKeySelector;
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

/**
 * This operator groups the graph elements based on the given grouping information into one element
 * and applies the given aggregation functions to the resulting element. This is done in a Window of
 * the stream.
 *
 * @param <W> type of window that is used in this operation
 */
public class Grouping<W extends Window> implements GraphToGraphOperatorI {

  private final GroupingInformation vertexGi;
  private final Set<AggregateFunction> vertexAggregateFunctions;
  private final GroupingInformation edgeGi;
  private final Set<AggregateFunction> edgeAggregateFunctions;

  private final WindowAssigner<Object, W> window;
  private final Trigger<Object, W> trigger;

  /**
   * Constructs the operator with the given grouping information, aggregation functions and the
   * window.
   *
   * @param vertexGi                 Grouping information for the vertices
   * @param vertexAggregateFunctions Aggregation functions for the grouped vertices
   * @param edgeGi                   Grouping information for the edges
   * @param edgeAggregateFunctions   Aggregation functions for the grouped edges
   * @param window                   window onto which this operator should be applied upon
   * @param trigger                  optional trigger
   */
  public Grouping(GroupingInformation vertexGi, Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, W> window,
      Trigger<Object, W> trigger) {
    this.vertexGi = vertexGi;
    this.vertexAggregateFunctions = vertexAggregateFunctions;
    this.edgeGi = edgeGi;
    this.edgeAggregateFunctions = edgeAggregateFunctions;
    this.window = window;
    this.trigger = trigger;
  }

  /**
   * Constructs the operator with the given grouping information (given as set), aggregation
   * functions and the window.
   *
   * @param vertexGiSet              set of keys (with {@link GroupingInformation#LABEL_SYMBOL if
   *                                 the element should be grouping should be applied on the label}
   *                                 for the vertices
   * @param vertexAggregateFunctions Aggregation functions for the grouped vertices
   * @param edgeGiSet                set of keys (with {@link GroupingInformation#LABEL_SYMBOL if
   *                                 the element should be grouping should be applied on the label}
   *                                 for the edges
   * @param edgeAggregateFunctions   Aggregation functions for the grouped edges
   * @param window                   window onto which this operator should be applied upon
   * @param trigger                  optional trigger
   */
  public Grouping(Set<String> vertexGiSet, Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGiSet, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, W> window,
      Trigger<Object, W> trigger) {
    this(new GroupingInformation(vertexGiSet),
        vertexAggregateFunctions,
        new GroupingInformation(edgeGiSet),
        edgeAggregateFunctions,
        window,
        trigger);
  }

  /**
   * Provides a builder which provides an more intuitive way to build a {@link Grouping}.
   *
   * @return a builder which provides an more intuitive way to build a {@link Grouping}.
   */
  public static GroupingBuilder createGrouping() {
    return new GroupingBuilder();
  }

  /**
   * Applies the grouping operator onto the stream
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the grouping operator applied
   */
  @Override
  public DataStream<Triplet> execute(DataStream<Triplet> stream) {
    return groupBy(stream);
  }

  /**
   * Applies the grouping onto the stream by applying all necessary DataStream-Operations.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the grouping operator applied
   */
  public DataStream<Triplet> groupBy(DataStream<Triplet> stream) {
    // Enrich stream with reverse edges
    var expandedStream = createStreamWithReverseEdges(stream);

    var aggregatedOnSourceStream = aggregateOnVertex(expandedStream, AggregateMode.SOURCE);
    var aggregatedOnVertexStream = aggregateOnVertex(aggregatedOnSourceStream,
        AggregateMode.TARGET);
    var reducedStream = aggregatedOnVertexStream.filter(ec -> !ec.getEdge().isReverse());
    var aggregatedOnEdgeStream = aggregateOnEdge(reducedStream);
    var graphId = GradoopId.get();
    return aggregatedOnEdgeStream.map(triplet -> {
      triplet.addGraphId(graphId);
      return triplet;
    });
  }

  /**
   * Creates a "reverse edge" for each edge in the stream and outputs both.
   *
   * @param stream stream which should be enriched by reverse edges
   * @return stream with reverse edges
   */
  private SingleOutputStreamOperator<Triplet> createStreamWithReverseEdges(
      DataStream<Triplet> stream) {
    return stream
        .flatMap(new FlatMapFunction<Triplet, Triplet>() {
          @Override
          public void flatMap(Triplet value, Collector<Triplet> out) {
            out.collect(value.createReverseTriplet());
            out.collect(value);
          }
        });
  }

  /**
   * Applies the aggregation process necessary to aggregate on one of the two vertices in an edge,
   * depending on the given mode.
   *
   * @param stream stream on which the vertices should be aggregated
   * @param mode   dictates which of the two vertices should be aggregated (i.e. for which vertex
   *               the grouping key is generated)
   * @return stream on which the indicated vertices are grouped
   */
  private DataStream<Triplet> aggregateOnVertex(DataStream<Triplet> stream,
      AggregateMode mode) {
    var windowedStream = createKeyedWindowedStream(stream, mode);
    return windowedStream.process(
        new VertexAggregation<>(vertexGi, vertexAggregateFunctions, mode));
  }

  /**
   * Applies the aggregation process necessary to aggregate on the edges
   *
   * @param stream stream on which the edges should be aggregated
   * @return stream on which the edges are grouped
   */
  private DataStream<Triplet> aggregateOnEdge(DataStream<Triplet> stream) {
    var windowedStream = createKeyedWindowedStream(stream, AggregateMode.EDGE);
    return windowedStream.process(new EdgeAggregation<W>(edgeGi, edgeAggregateFunctions));
  }

  /**
   * Applies operations necessary to make a keyed windowed stream
   *
   * @param es   stream on which the windowing should be applied
   * @param mode dictates on which element of the container (edge or one of the two vertices) should
   *             be keyed upon
   * @return stream that is keyed based on the given mode and windowed
   */
  private WindowedStream<Triplet, String, W> createKeyedWindowedStream(
      DataStream<Triplet> es, AggregateMode mode) {
    var windowedStream = es
        .keyBy(new TripletKeySelector(vertexGi, edgeGi, mode))
        .window(window);
    if (trigger != null) {
      windowedStream = windowedStream.trigger(trigger);
    }
    return windowedStream;
  }

  /**
   * Builder that provides an intuitive way to generate a {@link Grouping}-object.
   */
  public static final class GroupingBuilder {

    private final GroupingInformation vertexGi;
    private final Set<AggregateFunction> vertexAggFunctions;

    private final GroupingInformation edgeGi;
    private final Set<AggregateFunction> aggregateFunctions;

    /**
     * Constructs the initial state
     */
    public GroupingBuilder() {
      vertexGi = new GroupingInformation();
      vertexAggFunctions = new HashSet<>();
      edgeGi = new GroupingInformation();
      aggregateFunctions = new HashSet<>();
    }

    /**
     * Adds the given grouping key to the vertex grouping information
     *
     * @param vertexGroupingKey grouping key for vertices
     * @return the build with the given grouping key applied
     */
    public GroupingBuilder addVertexGroupingKey(String vertexGroupingKey) {
      vertexGi.addKey(vertexGroupingKey);
      return this;
    }

    /**
     * Adds the given grouping keys to the vertex grouping information
     *
     * @param vertexGroupingKeys set of grouping keys for vertices
     * @return the build with the given grouping keys applied
     */
    public GroupingBuilder addVertexGroupingKeys(Set<String> vertexGroupingKeys) {
      vertexGi.addKeys(vertexGroupingKeys);
      return this;
    }

    /**
     * Adds the given grouping key to the edge grouping information
     *
     * @param edgeGroupingKey grouping key for edges
     * @return the build with the given grouping key applied
     */
    public GroupingBuilder addEdgeGroupingKey(String edgeGroupingKey) {
      edgeGi.addKey(edgeGroupingKey);
      return this;
    }

    /**
     * Adds the given grouping keys to the vertex grouping information
     *
     * @param edgeGroupingKeys grouping keys for edges
     * @return the build with the given grouping keys applied
     */
    public GroupingBuilder addEdgeGroupingKeys(Set<String> edgeGroupingKeys) {
      edgeGi.addKeys(edgeGroupingKeys);
      return this;
    }

    /**
     * Builds the grouping with the given window.
     *
     * @param window window used for the grouping
     * @param <W>    type of window
     * @return grouping operator with the already provided grouping information and functions and
     * the given window
     */
    public <W extends Window> Grouping<W> buildWithWindow(WindowAssigner<Object, W> window) {
      return new Grouping<>(vertexGi, vertexAggFunctions, edgeGi, aggregateFunctions, window, null);
    }

    /**
     * Builds the grouping with the given window and trigger.
     * <p>
     * This method is experimental, as it was not tested and was originally intended for closing the
     * window after a specific amount of elements.
     *
     * @param window  window used for the grouping
     * @param trigger trigger that should be used
     * @param <W>     type of window
     * @return grouping operator with the already provided grouping information and functions and
     * the given window and trigger
     */
    @Beta
    public <W extends Window> Grouping<W> buildWithWindowAndTrigger(
        WindowAssigner<Object, W> window,
        Trigger<Object, W> trigger) {
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
