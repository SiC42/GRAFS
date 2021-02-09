package edu.leipzig.grafs.benchmark.tests.streaming.window;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.grouping.AllWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;

public class NoVertexGroupAllWindowedGroupingBenchmark extends AbstractWindowBenchmark {

  public NoVertexGroupAllWindowedGroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new NoVertexGroupAllWindowedGroupingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperatorWithWindow(GraphStream stream) {
    var groupingBuilder = AllWindowedGrouping.createGrouping()
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addEdgeAggregateFunction(new Count("used"));
    return stream.callForGraph(groupingBuilder.build()).withWindow(window).apply();
  }

}
