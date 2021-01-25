package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.operators.interfaces.window.WindowGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowGraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.union.UnionWithDuplicateInWindow;

public interface WindowedGCStreamOperators {

  /**
   * Creates a graph stream using the given operator.
   *
   * @param operator operator that should be used on this windowed graph collection stream
   * @return result of given operator as graph stream
   */
  GraphStream callForGraph(WindowGraphCollectionToGraphOperatorI operator);

  /**
   * Creates a graph collection stream using the given operator.
   *
   * @param operator operator that should be used on this windowed graph collection stream
   * @return result of given operator as graph collection stream
   */
  GCStream callForGC(WindowGraphCollectionToGraphCollectionOperatorI operator);

  // ---------------------------------------------------------------------------
  //  Operators
  // ---------------------------------------------------------------------------

  /**
   * Creates an triplet stream with the {@link UnionWithDuplicateInWindow} operator applied. Union of
   * two or more triplet streams creating a new stream containing all the elements from all the streams
   * with duplicates in a given window filtered out. No trigger is applied.
   *
   * @param streams the triplet streams to union output with
   * @return the unioned triplet stream
   */
  default GCStream unionWithDuplicateInWindow(GraphStream... streams) {
    return this.callForGC(new UnionWithDuplicateInWindow(streams));
  }

}
