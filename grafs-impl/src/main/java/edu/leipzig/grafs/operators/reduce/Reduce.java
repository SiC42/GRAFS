package edu.leipzig.grafs.operators.reduce;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphOperatorI;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

public class Reduce implements GraphCollectionToGraphOperatorI {


  protected GradoopId newId;


  public Reduce() {
    this.newId = GradoopId.get();
  }

  /**
   * Applies this operator on the stream and returns the stream with the operator applied.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the subgraph operator applied
   */
  @Override
  public DataStream<Triplet<Vertex, Edge>> execute(DataStream<Triplet<Vertex, Edge>> stream) {
    return stream.map(new ReduceFunction(newId)).name("Reduce Operator");
  }

  private static class ReduceFunction implements
      MapFunction<Triplet<Vertex, Edge>, Triplet<Vertex, Edge>> {

    protected GradoopId newId;

    public ReduceFunction(GradoopId id) {
      this.newId = id;
    }

    @Override
    public Triplet<Vertex, Edge> map(Triplet<Vertex, Edge> triplet) throws Exception {
      triplet.getEdge().setGraphIds(GradoopIdSet.fromExisting(newId));
      triplet.getSourceVertex().setGraphIds(GradoopIdSet.fromExisting(newId));
      triplet.getTargetVertex().setGraphIds(GradoopIdSet.fromExisting(newId));
      return triplet;
    }
  }

}
