package edu.leipzig.grafs.benchmark.tests.streaming.window;

import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.grouping.AllWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;

public class AllWindowedGroupingBenchmark extends AbstractWindowBenchmark {

  public AllWindowedGroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new AllWindowedGroupingBenchmark(args);
    benchmark.execute();
  }

  public AbstractStream<?> applyOperatorWithWindow(GraphStream stream) {
    var groupingBuilder = AllWindowedGrouping.createGrouping()
        .addVertexGroupingKey("name")
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addEdgeAggregateFunction(new Count("used"));
    return stream
        .window(window)
        .callForGraph(groupingBuilder.build());
  }

}
