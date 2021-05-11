package edu.leipzig.grafs.benchmark.tests.fixed;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.grouping.AllWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AllWindowedGroupingBenchmark extends AbstractFixedSizeBenchmark {

  public AllWindowedGroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new AllWindowedGroupingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperator(GraphStream stream) {
    var groupingBuilder = AllWindowedGrouping.createGrouping()
        .addVertexGroupingKey("name")
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addEdgeAggregateFunction(new Count("used"));
    return stream
        .window(TumblingEventTimeWindows.of(Time.minutes(2)))
        .callForGraph(groupingBuilder.build());
  }

}
