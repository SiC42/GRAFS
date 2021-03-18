package edu.leipzig.grafs.operators.matching.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.operators.grouping.model.ReversibleEdge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.jupiter.api.Test;

public class ReversibleEdgeTest {
  @Test
  public void createInvertedEdgeTest() {
    var edgeId = GradoopId.get();
    var sourceId = GradoopId.get();
    var targetId = GradoopId.get();
    var e = EdgeFactory.initEdge(edgeId, sourceId, targetId);
    var revE = ReversibleEdge.create(e,true);
    assertEquals(e.getSourceId(), revE.getTargetId());
    assertEquals(e.getTargetId(), revE.getSourceId());
  }

  @Test
  public void createNotInvertedReversibleEdgeTest() {
    var edgeId = GradoopId.get();
    var sourceId = GradoopId.get();
    var targetId = GradoopId.get();
    var e = EdgeFactory.initEdge(edgeId, sourceId, targetId);
    var revE = ReversibleEdge.create(e,false);
    assertEquals(e.getSourceId(), revE.getSourceId());
    assertEquals(e.getTargetId(), revE.getTargetId());
    assertEquals(e, revE.toEdge());
  }

}
