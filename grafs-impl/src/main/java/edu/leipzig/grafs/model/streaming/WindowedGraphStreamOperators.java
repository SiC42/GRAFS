package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowedGraphToGraphOperatorI;
import edu.leipzig.grafs.operators.matching.DualSimulation;
import edu.leipzig.grafs.operators.matching.Isomorphism;
import edu.leipzig.grafs.operators.union.UnionWithDuplicateInWindow;
import java.util.Set;

public interface WindowedGraphStreamOperators {

  /**
   * Creates a graph stream using the given operator.
   *
   * @param operator operator that should be used on this windowed graph stream
   * @return result of given operator as graph stream
   */
  GraphStream callForGraph(WindowedGraphToGraphOperatorI operator);

  /**
   * Creates a graph collection stream using the given operator.
   *
   * @param operator operator that should be used on this windowed graph stream
   * @return result of given operator as graph collection stream
   */
  GCStream callForGC(WindowedGraphToGraphCollectionOperatorI operator);

//   ---------------------------------------------------------------------------
//    Operators
//   ---------------------------------------------------------------------------

  /**
   * Creates an triplet stream with the {@link Grouping) operator applied.
   *
   * @param vertexGi                 vertex grouping information used for the operation
   * @param vertexAggregateFunctions vertex aggregation functions used for the operation
   * @param edgeGi                   edge grouping information used for the operation
   * @param edgeAggregateFunctions   edge aggregation functions used for the operation
   * @param window                   window on which the operation should be applied on
   * @param trigger                  trigger which should be applied to end the window for the
   *                                 operation
   * @return result stream of the grouping operator
   */
  default GraphStream grouping(GroupingInformation vertexGi,
      Set<AggregateFunction> vertexAggregateFunctions,
      GroupingInformation edgeGi, Set<AggregateFunction> edgeAggregateFunctions) {
    return this.callForGraph(
        new Grouping(vertexGi, vertexAggregateFunctions, edgeGi, edgeAggregateFunctions));
  }


  /**
   * Creates an triplet stream with the {@link DualSimulation} operator applied.
   *
   * @param gdlQueryStr query pattern with which the stream is matched
   * @return result stream of the matching operator
   */
  default GCStream dualSimulation(String gdlQueryStr) {
    return callForGC(new DualSimulation(gdlQueryStr));
  }

  /**
   * Creates an triplet stream with the {@link Isomorphism} matching operator applied.
   *
   * @param gdlQueryStr query pattern with which the stream is matched
   * @return result stream of the matching operator
   */
  default GCStream isomorphismMatching(String gdlQueryStr) {
    return callForGC(new Isomorphism(gdlQueryStr));
  }

  /**
   * Creates an triplet stream with the {@link UnionWithDuplicateInWindow} operator applied. Union of
   * two or more triplet streams creating a new stream containing all the elements from all the streams
   * with duplicates in a given window filtered out. No trigger is applied.
   *
   * @param streams the triplet streams to union output with
   * @return the unioned triplet stream
   */
  default GraphStream unionWithDuplicateInWindow(GraphStream... streams) {
    return this.callForGraph(new UnionWithDuplicateInWindow(streams));
  }

}
