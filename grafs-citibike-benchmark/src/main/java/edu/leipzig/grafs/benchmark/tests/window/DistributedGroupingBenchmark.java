package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.grouping.DistributedWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;

public class DistributedGroupingBenchmark extends AbstractWindowBenchmark {

  public DistributedGroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new DistributedGroupingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperatorWithWindow(GraphStream stream) {
    var groupingBuilder = DistributedWindowedGrouping.createGrouping()
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addEdgeAggregateFunction(new Count("used"));
    return stream.callForGraph(groupingBuilder.build()).withWindow(window).apply();
  }

}
