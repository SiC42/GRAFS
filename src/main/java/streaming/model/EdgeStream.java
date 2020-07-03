package streaming.model;

import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import streaming.operators.OperatorI;

public class EdgeStream implements EdgeStreamOperators {

  private DataStream<EdgeContainer> edgeStream;

  public EdgeStream(DataStream<EdgeContainer> edgeStream) {
    this.edgeStream = edgeStream.assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<EdgeContainer>() {
          @Override
          public long extractAscendingTimestamp(EdgeContainer edge) {
            return 0;
          }
        }
    );
  }

  public EdgeStream callForStream(OperatorI operator) {
    DataStream<EdgeContainer> result = operator.execute(edgeStream);
    return new EdgeStream(result);
  }

  public void print() {
    edgeStream.print();
  }

  public Iterator<EdgeContainer> collect() throws IOException {
    return DataStreamUtils.collect(edgeStream);
  }
}