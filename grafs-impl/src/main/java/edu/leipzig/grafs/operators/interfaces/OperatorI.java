package edu.leipzig.grafs.operators.interfaces;

import edu.leipzig.grafs.model.EdgeContainer;
import org.apache.flink.streaming.api.datastream.DataStream;

public interface OperatorI {

  DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream);

}
