package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.model.streaming.nonwindow.AbstractNonWindowedStream;
import edu.leipzig.grafs.model.streaming.window.WindowedGraphStream;
import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class GroupingBenchmark extends AbstractWindowBenchmark {

  public GroupingBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new GroupingBenchmark(args);
    benchmark.execute();
  }

  public <W extends Window> AbstractNonWindowedStream applyOperator(WindowedGraphStream<W> stream) {
    var groupingBuilder = Grouping.createGrouping()
        .addVertexGroupingKey("id")
        .addVertexAggregateFunction(new Count("used"))
        .addEdgeGroupingKey("bike_id")
        .addVertexAggregateFunction(new Count("used"));
    return stream.callForGraph(groupingBuilder.build());
  }

}
