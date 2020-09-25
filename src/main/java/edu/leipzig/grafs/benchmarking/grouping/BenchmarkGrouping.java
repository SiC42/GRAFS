package edu.leipzig.grafs.benchmarking.grouping;

import edu.leipzig.grafs.benchmarking.functions.SimpleMeter;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BenchmarkGrouping<W extends Window> extends Grouping<W> {

  private final String meterName;

  public BenchmarkGrouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    this(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions, window, trigger,
        "groupingMeter");
  }

  public BenchmarkGrouping(Set<String> vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGi, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    this(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions, window, trigger,
        "groupingMeter");
  }

  public BenchmarkGrouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi,
      Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger,
      String meterName) {
    super(vertexGi,
        vertexAggregateFunctions,
        edgeGi,
        edgeAggregateFunctions,
        window,
        trigger);
    this.meterName = meterName;
  }

  public BenchmarkGrouping(Set<String> vertexGiSet, Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGiSet, Set<AggregateFunction> edgeAggregateFunctions,
      WindowAssigner<Object, W> window, Trigger<EdgeContainer, W> trigger, String groupingMeter) {
    this(new GroupingInformation(vertexGiSet),
        vertexAggregateFunctions,
        new GroupingInformation(edgeGiSet),
        edgeAggregateFunctions,
        window,
        trigger,
        groupingMeter);
  }

  // TODO: Test if simple identity function will meter correctly
  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return super.execute(stream).map(new SimpleMeter<>(meterName));
  }


}
