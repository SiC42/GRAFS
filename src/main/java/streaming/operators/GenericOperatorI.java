package streaming.operators;

import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;

public interface GenericOperatorI<IType, OType> {

 DataStream<OType> execute(DataStream<IType> stream);

}
