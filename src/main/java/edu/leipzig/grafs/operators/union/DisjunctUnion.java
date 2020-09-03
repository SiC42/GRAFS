package edu.leipzig.grafs.operators.union;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.streaming.api.datastream.DataStream;

public class DisjunctUnion implements GraphToGraphOperatorI, GraphToGraphCollectionOperatorI {

  private final EdgeStream[] streams;

  public DisjunctUnion(EdgeStream... streams){
    this.streams = streams;
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    var dataStreams = new DataStream[streams.length];
    for (int i = 0; i < streams.length; i++) {
      dataStreams[i] = streams[i].getDataStream();
    }
    @SuppressWarnings("unchecked")
    DataStream<EdgeContainer> mergedStream = stream.union(dataStreams);
    return mergedStream;
  }
}
