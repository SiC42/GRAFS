package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;

public class SimpleGroupingBenchmark extends AbstractWindowBenchmark {

  public SimpleGroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new SimpleGroupingBenchmark(args);
    benchmark.execute();
  }

  public EdgeStream applyOperator() {
    var groupingBuilder = Grouping.createGrouping()
        .addVertexGroupingKey("id")
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addVertexAggregateFunction(new Count("used"));
    if (useTrigger) {
      return this.edgeStream.callForStream(
          groupingBuilder.buildWithWindowAndTrigger(window, countTrigger));
    } else {
      return this.edgeStream.callForStream(
          groupingBuilder.buildWithWindow(window));
    }
  }

}
