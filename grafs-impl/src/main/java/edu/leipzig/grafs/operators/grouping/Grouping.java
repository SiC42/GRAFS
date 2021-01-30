package edu.leipzig.grafs.operators.grouping;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.model.window.AbstractTumblingWindows;
import edu.leipzig.grafs.model.window.WindowsI;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.logic.EdgeAggregation;
import edu.leipzig.grafs.operators.grouping.logic.TripletKeySelector;
import edu.leipzig.grafs.operators.grouping.logic.VertexAggregation;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphToGraphOperatorI;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * This operator groups the graph elements based on the given grouping information into one element
 * and applies the given aggregation functions to the resulting element. This is done in a Window of
 * the stream.
 */
public class Grouping extends AbstractGrouping<AbstractTumblingWindows> {

  /**
   * Constructs the operator with the given grouping information, aggregation functions and the
   * window.
   *
   * @param vertexGi                 Grouping information for the vertices
   * @param vertexAggregateFunctions Aggregation functions for the grouped vertices
   * @param edgeGi                   Grouping information for the edges
   * @param edgeAggregateFunctions   Aggregation functions for the grouped edges
   */
  public Grouping(GroupingInformation vertexGi, Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions) {
    super(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions);
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
   */
  public Grouping(Set<String> vertexGiSet, Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGiSet, Set<AggregateFunction> edgeAggregateFunctions) {
    super(vertexGiSet, vertexAggregateFunctions, edgeGiSet, edgeAggregateFunctions);
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
   * Applies the grouping onto the stream by applying all necessary DataStream-Operations.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the grouping operator applied
   */
  public <W extends Window> DataStream<Triplet> groupBy(DataStream<Triplet> stream,
      WindowingInformation<W> wi) {
    // Enrich stream with reverse edges
    var expandedStream = createStreamWithReverseEdges(stream);

    var aggregatedOnSourceStream = aggregateOnVertex(expandedStream, AggregateMode.SOURCE, wi);
    var aggregatedOnVertexStream = aggregateOnVertex(aggregatedOnSourceStream,
        AggregateMode.TARGET, wi);
    var reducedStream = aggregatedOnVertexStream.filter(ec -> !ec.getEdge().isReverse());
    var aggregatedOnEdgeStream = aggregateOnEdge(reducedStream, wi);
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
        }).name("Create Reverse Edges");
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
  private <W extends Window> DataStream<Triplet> aggregateOnVertex(DataStream<Triplet> stream,
      AggregateMode mode, WindowingInformation<W> wi) {
    var windowedStream = createKeyedWindowedStream(stream, vertexGi, mode, wi);
    return windowedStream.process(
        new VertexAggregation<>(vertexGi, vertexAggregateFunctions, mode))
        .name("Aggregate " + mode.name() + " VERTICES");
  }

  /**
   * Applies the aggregation process necessary to aggregate on the edges
   *
   * @param stream stream on which the edges should be aggregated
   * @return stream on which the edges are grouped
   */
  private <W extends Window> DataStream<Triplet> aggregateOnEdge(DataStream<Triplet> stream,
      WindowingInformation<W> wi) {
    var windowedStream = createKeyedWindowedStream(stream, edgeGi, AggregateMode.EDGE, wi);
    return windowedStream
        .process(new EdgeAggregation<>(edgeGi, edgeAggregateFunctions, GradoopId.get()))
        .name("Aggregate EDGES");
  }

  /**
   * Applies operations necessary to make a keyed windowed stream
   *
   * @param es   stream on which the windowing should be applied
   * @param mode dictates on which element of the container (edge or one of the two vertices) should
   *             be keyed upon
   * @return stream that is keyed based on the given mode and windowed
   */
  private <W extends Window> WindowedStream<Triplet, String, W> createKeyedWindowedStream(
      DataStream<Triplet> es, GroupingInformation gi, AggregateMode mode,
      WindowingInformation<W> wi) {
    var windowedStream = es
        .keyBy(new TripletKeySelector(gi, mode))
        .window(wi.getWindow());
    windowedStream = applyOtherWindowInformation(windowedStream, wi);
    return windowedStream;
  }

  /**
   * Builder that provides an intuitive way to generate a {@link Grouping}-object.
   */
  public static final class GroupingBuilder extends AbstractGroupingBuilder<AbstractTumblingWindows> {


    /**
     * Builds the grouping.
     *
     * @return grouping operator with the already provided grouping information and functions
     */
    public Grouping build() {
      return new Grouping(vertexGi, vertexAggFunctions, edgeGi, aggregateFunctions);
    }
  }
}
