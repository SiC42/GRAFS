package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.operators.interfaces.GraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.reduce.Reduce;
import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.id.GradoopIdSet;

public interface GCStreamOperators {
  /**
   * Creates an edge stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  GraphStream callForGraph(GraphCollectionToGraphOperatorI operator);


}
