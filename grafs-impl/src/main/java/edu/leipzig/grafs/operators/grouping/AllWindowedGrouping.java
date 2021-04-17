package edu.leipzig.grafs.operators.grouping;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.logic.AllWindowAggregation;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * This operator groups the graph elements based on the given grouping information into one element
 * and applies the given aggregation functions to the resulting element. This is done in a Window of
 * the stream.
 *
 * This is a centralized version, i.e. all elements in a window are processed in one task slot (/one worker).
 */
public class AllWindowedGrouping extends AbstractWindowedGrouping {


  /**
   * Constructs the operator with the given grouping information, aggregation functions and the
   * window.
   *
   * @param vertexGi                 Grouping information for the vertices
   * @param vertexAggregateFunctions Aggregation functions for the grouped vertices
   * @param edgeGi                   Grouping information for the edges
   * @param edgeAggregateFunctions   Aggregation functions for the grouped edges
   */
  public AllWindowedGrouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
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
  public AllWindowedGrouping(Set<String> vertexGiSet,
      Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGiSet, Set<AggregateFunction> edgeAggregateFunctions) {
    super(vertexGiSet, vertexAggregateFunctions, edgeGiSet, edgeAggregateFunctions);
  }

  /**
   * Provides a builder which provides an more intuitive way to build a {@link
   * AllWindowedGrouping}.
   *
   * @return a builder which provides an more intuitive way to build a {@link AllWindowedGrouping}.
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
  public <FW extends Window> DataStream<Triplet<Vertex, Edge>> groupBy(
      DataStream<Triplet<Vertex, Edge>> stream,
      WindowingInformation<FW> wi) {
    var windowedStream = createWindowedStream(stream, wi);
    return windowedStream.process(
        new AllWindowAggregation<>(vertexGi, vertexAggregateFunctions, edgeGi,
            edgeAggregateFunctions));
  }

  private <W extends Window> AllWindowedStream<Triplet<Vertex, Edge>, W> createWindowedStream(
      DataStream<Triplet<Vertex, Edge>> es, WindowingInformation<W> wi) {
    var windowedStream = es
        .windowAll(wi.getWindow());
    windowedStream = applyOtherWindowInformation(windowedStream, wi);
    return windowedStream;
  }

  /**
   * Builder that provides an intuitive way to generate a {@link DistributedWindowedGrouping}-object.
   */
  public static final class GroupingBuilder extends AbstractGroupingBuilder {

    /**
     * Builds the grouping.
     *
     * @return grouping operator with the already provided grouping information and functions
     */
    public AllWindowedGrouping build() {
      return new AllWindowedGrouping(vertexGi, vertexAggFunctions, edgeGi, aggregateFunctions);
    }
  }
}
