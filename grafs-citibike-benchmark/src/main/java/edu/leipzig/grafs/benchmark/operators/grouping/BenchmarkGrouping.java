package edu.leipzig.grafs.benchmark.operators.grouping;

import edu.leipzig.grafs.benchmark.operators.functions.SimpleMeter;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.AbstractWindowedStream.WindowInformation;
import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class BenchmarkGrouping extends Grouping {

  private final String meterName;

  public BenchmarkGrouping(Set<String> vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGi, Set<AggregateFunction> edgeAggregateFunctions) {
    this(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions, "groupingMeter");
  }

  public BenchmarkGrouping(GroupingInformation vertexGi,
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

  public BenchmarkGrouping(Set<String> vertexGiSet, Set<AggregateFunction> vertexAggregateFunctions,
      Set<String> edgeGiSet, Set<AggregateFunction> edgeAggregateFunctions, String groupingMeter) {
    this(new GroupingInformation(vertexGiSet),
        vertexAggregateFunctions,
        new GroupingInformation(edgeGiSet),
        edgeAggregateFunctions,
        groupingMeter);
  }

  // TODO: Test if simple identity function will meter correctly
  @Override
  public <W extends Window> DataStream<Triplet> execute(DataStream<Triplet> stream,
      WindowInformation<W> wi) {
    return super.execute(stream, wi).map(new SimpleMeter<>(meterName));
  }


}
