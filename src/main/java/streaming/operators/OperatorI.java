package streaming.operators;

import org.apache.flink.streaming.api.datastream.DataStream;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.EdgeStream;

public interface OperatorI extends GenericOperatorI<EdgeContainer, EdgeContainer> {

}
