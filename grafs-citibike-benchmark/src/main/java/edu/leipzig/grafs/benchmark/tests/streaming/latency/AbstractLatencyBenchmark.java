package edu.leipzig.grafs.benchmark.tests.streaming.latency;

import edu.leipzig.grafs.benchmark.tests.streaming.AbstractStreamingBenchmark;
import edu.leipzig.grafs.operators.transform.EdgeTransformation;
import java.util.concurrent.TimeUnit;

public abstract class AbstractLatencyBenchmark extends AbstractStreamingBenchmark {

  public AbstractLatencyBenchmark(String[] args) {
    super(args);
  }


  @Override
  public void execute() throws Exception {
    var timestampKey = "timestamp";
    var baseStream = this.stream
        .callForGraph(new EdgeTransformation(e -> {
          e.setProperty(timestampKey, System.currentTimeMillis());
          return e;
        }));
    var graphStream = applyOperator(baseStream);
    var datastream = stream.getDataStream();
    datastream.map(t -> {
      var startTime = t.getEdge().getPropertyValue(timestampKey).getLong();
      return System.currentTimeMillis() - startTime;
    });
    var path = properties.getProperty(OUTPUT_PATH_KEY);
    datastream.writeAsText(path);
    var result = env.execute(properties.getProperty(OPERATOR_NAME_KEY));
    var timeInMilliSeconds = result.getNetRuntime(TimeUnit.MILLISECONDS);
    outputWriter.write(getCsvLine(timeInMilliSeconds));
    outputWriter.flush();
    outputWriter.close();
  }
}
