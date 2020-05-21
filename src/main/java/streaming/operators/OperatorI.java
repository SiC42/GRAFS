package streaming.operators;

import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeStream;

public interface OperatorI {

 EdgeStream execute(DataStream<Edge> stream);

}
