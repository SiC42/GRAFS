package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.reduce.Reduce;
import edu.leipzig.grafs.operators.union.DisjunctUnion;
import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopIdSet;

public interface GCStreamOperators {

  /**
   * Creates a graph stream using the given operator.
   *
   * @param operator operator that should be used on this graph collection stream
   * @return result of given operator as graph stream
   */
  GraphStream callForGraph(GraphCollectionToGraphOperatorI operator);

  /**
   * Creates a graph collection stream using the given operator.
   *
   * @param operator operator that should be used on this graph collection stream
   * @return result of given operator as graph collection stream
   */
  GCStream callForGC(GraphCollectionToGraphCollectionOperatorI operator);


  // ---------------------------------------------------------------------------
  //  Operators
  // ---------------------------------------------------------------------------

  /**
   * Applies the Reduce Operator onto the graph collection stream, creating a graph stream.
   *
   * @return result of given operator
   */
  default GraphStream reduce() {
    return callForGraph(new Reduce());
  }

  /**
   * Creates an triplet stream with the {@link DisjunctUnion} operator applied. Represents a union
   * of two or more triplet streams creating a new stream containing all the elements from all the
   * streams.
   * <p>
   * Note: This operator assumes that the streams are disjunct, i.e. no element in both streams is
   * present in the other stream.
   *
   * @param streams the triplet streams to union output with
   * @return the unioned triplet stream
   */
  default GCStream disjunctUnion(GraphStream... streams) {
    return this.callForGC(new DisjunctUnion(streams));
  }

}
