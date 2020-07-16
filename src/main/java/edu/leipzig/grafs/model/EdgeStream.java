package edu.leipzig.grafs.model;

import edu.leipzig.grafs.operators.OperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;

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

  @Override
  public EdgeStream union(EdgeStream otherStream) {
    var mergedStream = this.edgeStream.union(otherStream.edgeStream);
    return new EdgeStream(mergedStream, config);
  }

  public void print() {
    edgeStream.print();
  }

  public Iterator<EdgeContainer> collect() throws IOException {
    return DataStreamUtils.collect(edgeStream);
  }
}