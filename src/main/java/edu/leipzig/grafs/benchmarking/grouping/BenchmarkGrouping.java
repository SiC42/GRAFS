package edu.leipzig.grafs.benchmarking.grouping;

import edu.leipzig.grafs.benchmarking.generic.SimpleMeter;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BenchmarkGrouping<W extends Window> extends Grouping<W> {

  private final String meterName;

  public BenchmarkGrouping(GroupingInformation vertexGi,
      AggregationMapping vertexAggMap,
      GroupingInformation edgeGi,
      AggregationMapping edgeAggMap,
      WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    this(vertexGi, vertexAggMap, edgeGi, edgeAggMap, window, trigger, "groupingMeter");
  }

  public BenchmarkGrouping(GroupingInformation vertexGi,
      AggregationMapping vertexAggMap,
      GroupingInformation edgeGi,
      AggregationMapping edgeAggMap,
      WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger,
      String meterName) {
    super(vertexGi, vertexAggMap, edgeGi, edgeAggMap, window, trigger);
    this.meterName = meterName;
  }

  // TODO: Test if simple identity function will meter correctly
  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    return super.execute(stream).map(new SimpleMeter<>(meterName));
  }


}
