package edu.leipzig.grafs.operators.union;

import com.google.common.annotations.Beta;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * This operator unifies multiple streams into the given one. It is implied, that the set of
 * elements in both streams are disjunct (or it is not of importance for the analysis.
 */
@Beta
public class DisjunctUnion implements GraphToGraphOperatorI, GraphToGraphCollectionOperatorI {

  /**
   * Streams that should be unified via the union operation.
   */
  private final EdgeStream[] streams;

  /**
   * Initializes this operator with the given streams.
   *
   * @param streams streams that should be unified by applying the union operator onto a stream
   */
  public DisjunctUnion(EdgeStream... streams) {
    this.streams = streams;
  }

  /**
   * Applies the streams to this stream and returns the unified stream
   *
   * @param stream stream on which the operator should be applied
   * @return unified stream
   */
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
