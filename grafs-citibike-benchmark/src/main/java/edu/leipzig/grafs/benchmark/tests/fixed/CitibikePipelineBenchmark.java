package edu.leipzig.grafs.benchmark.tests.fixed;

import edu.leipzig.grafs.benchmark.tests.streaming.latency.AbstractWindowBenchmark;
import edu.leipzig.grafs.benchmark.utils.GeoUtils;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.model.window.TumblingEventTimeWindows;
import edu.leipzig.grafs.operators.grouping.DistributedWindowedGrouping;
import edu.leipzig.grafs.operators.grouping.functions.Count;
import edu.leipzig.grafs.operators.subgraph.Subgraph;
import edu.leipzig.grafs.operators.subgraph.Subgraph.Strategy;
import edu.leipzig.grafs.operators.transform.VertexTransformation;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CitibikePipelineBenchmark extends AbstractFixedSizeBenchmark {

  public CitibikePipelineBenchmark(String[] args) {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    var benchmark = new CitibikePipelineBenchmark(args);
    benchmark.execute();
  }

  @Override
  public AbstractStream<?> applyOperator(GraphStream stream) {
    var gridCellKey = "gridCell";
    var transformedStream = stream
        .callForGraph(new VertexTransformation(v -> {
          var lat = v.getPropertyValue("lat").getFloat();
          var lon = v.getPropertyValue("long").getFloat();
          var gridCell = GeoUtils.mapToGridCell(lon, lat);
          v.setProperty(gridCellKey, gridCell);
          return v;
        }));
    var groupedStream = transformedStream
        .callForGraph(DistributedWindowedGrouping.createGrouping()
            .addVertexGroupingKey(gridCellKey)
            .addVertexAggregateFunction(new Count("stationsInGridCell"))
            .build()
        )
        .withWindow(TumblingEventTimeWindows.of(Time.minutes(2)))
        .apply();

    return groupedStream
        .callForGraph(new Subgraph(v -> v.getPropertyValue(gridCellKey).getInt() > 10, null,
            Strategy.VERTEX_INDUCED));
  }
}
