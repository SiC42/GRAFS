package edu.leipzig.grafs.operators.grouping;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.model.window.WindowsI;
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
 */
public class AllWindowGrouping extends AbstractGrouping<WindowsI<? extends Window>> {


  /**
   * Constructs the operator with the given grouping information, aggregation functions and the
   * window.
   *
   * @param vertexGi                 Grouping information for the vertices
   * @param vertexAggregateFunctions Aggregation functions for the grouped vertices
   * @param edgeGi                   Grouping information for the edges
   * @param edgeAggregateFunctions   Aggregation functions for the grouped edges
   */
  public AllWindowGrouping(GroupingInformation vertexGi,
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
  public AllWindowGrouping(Set<String> vertexGiSet, Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGiSet, Set<AggregateFunction> edgeAggregateFunctions) {
    super(vertexGiSet, vertexAggregateFunctions, edgeGiSet, edgeAggregateFunctions);
  }

  /**
   * Provides a builder which provides an more intuitive way to build a {@link AllWindowGrouping}.
   *
   * @return a builder which provides an more intuitive way to build a {@link AllWindowGrouping}.
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
  public <FW extends Window> DataStream<Triplet> groupBy(DataStream<Triplet> stream,
      WindowingInformation<FW> wi) {
    var windowedStream = createWindowedStream(stream, wi);
    return windowedStream.process(new AllWindowAggregation<>(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions));
  }

  private <W extends Window> AllWindowedStream<Triplet, W> createWindowedStream(
      DataStream<Triplet> es, WindowingInformation<W> wi) {
    var windowedStream = es
        .windowAll(wi.getWindow());
    windowedStream = applyOtherWindowInformation(windowedStream, wi);
    return windowedStream;
  }

  /**
   * Builder that provides an intuitive way to generate a {@link Grouping}-object.
   */
  public static final class GroupingBuilder extends AbstractGroupingBuilder<WindowsI<?>> {

    /**
     * Builds the grouping.
     *
     * @return grouping operator with the already provided grouping information and functions
     */
    public AllWindowGrouping build() {
      return new AllWindowGrouping(vertexGi, vertexAggFunctions, edgeGi, aggregateFunctions);
    }
  }
}
