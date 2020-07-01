package streaming.operators;

import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.EdgeContainer;

public interface OperatorI {

  DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream);

}
