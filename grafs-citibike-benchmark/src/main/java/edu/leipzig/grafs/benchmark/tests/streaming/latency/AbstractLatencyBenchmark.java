package edu.leipzig.grafs.benchmark.tests.streaming.latency;

import edu.leipzig.grafs.benchmark.tests.streaming.AbstractStreamingBenchmark;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.transform.EdgeTransformation;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.MapFunction;

public abstract class AbstractLatencyBenchmark extends AbstractStreamingBenchmark {

  public static final String TIMESTAMP_KEY = "timestamp";

  public AbstractLatencyBenchmark(String[] args) {
    super(args);
  }


  @Override
  public void execute() throws Exception {
    var dataStream = this.stream.getDataStream();
    dataStream = dataStream.map(new MapFunction<Triplet<Vertex, Edge>, Triplet<Vertex, Edge>>() {

      @Override
      public Triplet<Vertex, Edge> map(Triplet<Vertex, Edge> triplet) throws Exception {
        triplet.getEdge().setProperty(TIMESTAMP_KEY, System.currentTimeMillis());
        return triplet;
      }
    }).name("Set Timestamp").setParallelism(96);
    var graphStream = applyOperator(new GraphStream(dataStream, config));
    var afterOpStream = graphStream.getDataStream();
    var longStream = afterOpStream.map(new MapFunction<Triplet<Vertex, Edge>, Long>() {
      @Override
      public Long map(Triplet<Vertex, Edge> triplet) throws Exception {
          var startTime = triplet.getEdge().getPropertyValue(TIMESTAMP_KEY).getLong();
          return System.currentTimeMillis() - startTime;
      }
    }).setParallelism(96).name("Extracting Latency");
    var path = properties.getProperty(OUTPUT_PATH_KEY);
    longStream.writeAsText(path).setParallelism(96).name("Write to file sink");
    var result = env.execute(properties.getProperty(OPERATOR_NAME_KEY));
    var timeInMilliSeconds = result.getNetRuntime(TimeUnit.MILLISECONDS);
    outputWriter.write(getCsvLine(timeInMilliSeconds));
    outputWriter.flush();
    outputWriter.close();
  }
}
