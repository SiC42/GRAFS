package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.operators.interfaces.GraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.reduce.Reduce;
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

  /**
   * Applies the Reduce Operator onto the graph collection stream, creating a graph stream
   *
   * @param idSetFilter defines a filter to filter out elements that shouldn't be part of the graph
   *                    stream
   * @return result of given operator
   */
  default GraphStream reduce(FilterFunction<GradoopIdSet> idSetFilter) {
    return callForGraph(new Reduce(idSetFilter));
  }

}
