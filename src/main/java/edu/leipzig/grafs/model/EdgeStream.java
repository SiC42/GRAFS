package edu.leipzig.grafs.model;

import edu.leipzig.grafs.operators.interfaces.OperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class EdgeStream implements EdgeStreamOperators {

  private final DataStream<EdgeContainer> edgeStream;
  private final FlinkConfig config;


  public EdgeStream(DataStream<EdgeContainer> edgeStream, FlinkConfig config) {
    this.edgeStream = edgeStream.assignTimestampsAndWatermarks(config.getWatermarkStrategy());
    this.config = config;
  }

  public static EdgeStream fromSource(FlinkConfig config,
      FlinkKafkaConsumer<EdgeContainer> fkConsumer) {
    var stream = config.getExecutionEnvironment().addSource(fkConsumer);
    return new EdgeStream(stream, config);
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