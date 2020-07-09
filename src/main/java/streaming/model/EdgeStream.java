package streaming.model;

import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import streaming.operators.OperatorI;
import streaming.util.FlinkConfig;

public class EdgeStream implements EdgeStreamOperators {

  private final DataStream<EdgeContainer> edgeStream;
  private final FlinkConfig config;


  public EdgeStream(DataStream<EdgeContainer> edgeStream, FlinkConfig config) {
    this.edgeStream = edgeStream;
    this.config = config;

  }

  public EdgeStream callForStream(OperatorI operator) {
    DataStream<EdgeContainer> result = operator.execute(edgeStream);
    return new EdgeStream(result, config);
  }

  public void print() {
    edgeStream.print();
  }

  public Iterator<EdgeContainer> collect() throws IOException {
    return DataStreamUtils.collect(edgeStream);
  }
}