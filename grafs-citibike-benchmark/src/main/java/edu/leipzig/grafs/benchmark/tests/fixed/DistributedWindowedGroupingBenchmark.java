package edu.leipzig.grafs.benchmark.tests.fixed;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.grouping.DistributedWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DistributedWindowedGroupingBenchmark extends AbstractFixedSizeBenchmark {

  public DistributedWindowedGroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new DistributedWindowedGroupingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    var groupingBuilder = DistributedWindowedGrouping.createGrouping()
        .addVertexGroupingKey("name")
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addEdgeAggregateFunction(new Count("used"));
    return stream.callForGraph(groupingBuilder.build())
        .withWindow(TumblingEventTimeWindows.of(Time.minutes(2)))
        .apply();
  }

}
