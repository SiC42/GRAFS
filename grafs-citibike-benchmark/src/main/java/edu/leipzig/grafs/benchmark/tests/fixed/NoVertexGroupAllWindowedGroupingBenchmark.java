package edu.leipzig.grafs.benchmark.tests.fixed;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.grouping.AllWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class NoVertexGroupAllWindowedGroupingBenchmark extends AbstractFixedSizeBenchmark {

  public NoVertexGroupAllWindowedGroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new NoVertexGroupAllWindowedGroupingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    var groupingBuilder = AllWindowedGrouping.createGrouping()
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addEdgeAggregateFunction(new Count("used"));
    return stream.callForGraph(groupingBuilder.build())
        .withWindow(TumblingEventTimeWindows.of(Time.minutes(2)))
        .apply();
  }

}
