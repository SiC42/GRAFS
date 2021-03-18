package edu.leipzig.grafs.operators.matching.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.operators.grouping.model.ReversableEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.jupiter.api.Test;

public class ReversableEdgeTest {
  @Test
  public void createInvertedEdgeTest() {
    var edgeId = GradoopId.get();
    var sourceId = GradoopId.get();
    var targetId = GradoopId.get();
    var e = EdgeFactory.initEdge(edgeId, sourceId, targetId);
    var revE = ReversableEdge.create(e,true);
    assertEquals(e.getSourceId(), revE.getTargetId());
    assertEquals(e.getTargetId(), revE.getSourceId());
  }

  @Test
  public void createNotInvertedReversableEdgeTest() {
    var edgeId = GradoopId.get();
    var sourceId = GradoopId.get();
    var targetId = GradoopId.get();
    var e = EdgeFactory.initEdge(edgeId, sourceId, targetId);
    var revE = ReversableEdge.create(e,false);
    assertEquals(e.getSourceId(), revE.getSourceId());
    assertEquals(e.getTargetId(), revE.getTargetId());
    assertEquals(e, revE.toEdge());
  }

}
