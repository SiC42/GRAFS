package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphCollectionToGraphOperatorI;

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

}
