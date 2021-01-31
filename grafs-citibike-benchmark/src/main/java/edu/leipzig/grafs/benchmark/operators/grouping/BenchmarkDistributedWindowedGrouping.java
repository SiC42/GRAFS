package edu.leipzig.grafs.benchmark.operators.grouping;

import edu.leipzig.grafs.benchmark.operators.functions.SimpleMeter;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.operators.grouping.DistributedWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BenchmarkDistributedWindowedGrouping extends DistributedWindowedGrouping {

  private final String meterName;

  public BenchmarkDistributedWindowedGrouping(Set<String> vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGi, Set<AggregateFunction> edgeAggregateFunctions) {
    this(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions, "groupingMeter");
  }

  public BenchmarkDistributedWindowedGrouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi,
      Set<AggregateFunction> edgeAggregateFunctions,
      String meterName) {
    super(vertexGi,
        vertexAggregateFunctions,
        edgeGi,
        edgeAggregateFunctions);
    this.meterName = meterName;
  }

  public BenchmarkDistributedWindowedGrouping(Set<String> vertexGiSet, Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGiSet, Set<AggregateFunction> edgeAggregateFunctions, String groupingMeter) {
    this(new GroupingInformation(vertexGiSet),
        vertexAggregateFunctions,
        new GroupingInformation(edgeGiSet),
        edgeAggregateFunctions,
        groupingMeter);
  }

  @Override
  public <FW extends Window> DataStream<Triplet> execute(DataStream<Triplet> stream,
      WindowingInformation<FW> wi) {
    return super.execute(stream, wi).map(new SimpleMeter<>(meterName));
  }
}
