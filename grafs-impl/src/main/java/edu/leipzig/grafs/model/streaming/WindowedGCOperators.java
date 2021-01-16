package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphToGraphOperatorI;

public interface WindowedGCOperators {

  /**
   * Creates an edge stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  GraphStream callForGraph(WindowedGraphCollectionToGraphOperatorI operator);

  /**
   * Creates an edge stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  GCStream callForGC(WindowedGraphCollectionToGraphCollectionOperatorI operator);

}
