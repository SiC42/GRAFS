package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.union.UnionWithDuplicateInWindow;

public interface WindowedGCOperators {

  /**
   * Creates a graph stream using the given operator.
   *
   * @param operator operator that should be used on this windowed graph collection stream
   * @return result of given operator as graph stream
   */
  GraphStream callForGraph(WindowedGraphCollectionToGraphOperatorI operator);

  /**
   * Creates a graph collection stream using the given operator.
   *
   * @param operator operator that should be used on this windowed graph collection stream
   * @return result of given operator as graph collection stream
   */
  GCStream callForGC(WindowedGraphCollectionToGraphCollectionOperatorI operator);

  // ---------------------------------------------------------------------------
  //  Operators
  // ---------------------------------------------------------------------------

  /**
   * Creates an edge stream with the {@link UnionWithDuplicateInWindow} operator applied. Union of
   * two or more edge streams creating a new stream containing all the elements from all the streams
   * with duplicates in a given window filtered out. No trigger is applied.
   *
   * @param streams the edge streams to union output with
   * @return the unioned edge stream
   */
  default GCStream unionWithDuplicateInWindow(GraphStream... streams) {
    return this.callForGC(new UnionWithDuplicateInWindow(streams));
  }

}
