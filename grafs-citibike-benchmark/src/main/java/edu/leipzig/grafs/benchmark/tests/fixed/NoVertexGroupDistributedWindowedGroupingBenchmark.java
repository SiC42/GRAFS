package edu.leipzig.grafs.benchmark.tests.fixed;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.model.window.TumblingEventTimeWindows;
import edu.leipzig.grafs.operators.grouping.DistributedWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;
import org.apache.flink.streaming.api.windowing.time.Time;

public class NoVertexGroupDistributedWindowedGroupingBenchmark extends AbstractFixedSizeBenchmark {

  public NoVertexGroupDistributedWindowedGroupingBenchmark(String[] args) {
    super(args);
  }


  public static void main(String[] args) throws Exception {
    var benchmark = new NoVertexGroupDistributedWindowedGroupingBenchmark(args);
    benchmark.execute();
  }

  @Override
  public AbstractStream<?> applyOperator(GraphStream stream) {
    var groupingBuilder = DistributedWindowedGrouping.createGrouping()
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addEdgeAggregateFunction(new Count("used"));
    return stream.callForGraph(groupingBuilder.build())
        .withWindow(TumblingEventTimeWindows.of(Time.minutes(2)))
        .apply();
  }


}
