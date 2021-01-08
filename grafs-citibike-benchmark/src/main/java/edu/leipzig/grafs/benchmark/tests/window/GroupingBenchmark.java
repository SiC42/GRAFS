package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;

public class GroupingBenchmark extends AbstractWindowBenchmark {

  public GroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new GroupingBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator(EdgeStream edgeStream) {
    var groupingBuilder = Grouping.createGrouping()
        .addVertexGroupingKey("id")
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addVertexAggregateFunction(new Count("used"));
    if (useTrigger) {
      return edgeStream.callForStream(
          groupingBuilder.buildWithWindowAndTrigger(window, countTrigger));
    } else {
      return edgeStream.callForStream(
          groupingBuilder.buildWithWindow(window));
    }
  }

}