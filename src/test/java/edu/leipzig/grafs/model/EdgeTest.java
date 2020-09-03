package edu.leipzig.grafs.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.leipzig.grafs.factory.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.hamcrest.core.Is;
import org.junit.jupiter.api.Test;

public class EdgeTest {

  @Test
  public void createWithIDTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    Edge e = EdgeFactory.initEdge(edgeId, sourceId, targetId);
    assertThat(e.getId(), is(edgeId));
    assertThat(e.getSourceId(), is(sourceId));
    assertThat(e.getTargetId(), is(targetId));
    assertThat(e.getPropertyCount(), is(0));
    assertThat(e.getGraphCount(), is(0));
  }

  @Test
  public void createEdgePojoTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    GradoopIdSet graphIds = GradoopIdSet
        .fromExisting(GradoopId.get(), GradoopId.get());

    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    Edge edge = EdgeFactory.initEdge(edgeId, label, sourceId, targetId, props, graphIds);

    assertThat(edge.getId(), is(edgeId));
    assertEquals(label, edge.getLabel());
    assertThat(edge.getSourceId(), is(sourceId));
    assertThat(edge.getTargetId(), is(targetId));
    assertThat(edge.getPropertyCount(), is(2));
    assertThat(edge.getPropertyValue("k1").getString(), Is.<Object>is("v1"));
    assertThat(edge.getPropertyValue("k2").getString(), Is.<Object>is("v2"));
    assertThat(edge.getGraphCount(), is(2));

    for (GradoopId graphId : graphIds) {
      assertTrue(edge.getGraphIds().contains(graphId));
    }
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    Edge e =
        EdgeFactory.initEdge(edgeId, sourceId, targetId);
    assertThat(e.getLabel(), is(GradoopConstants.DEFAULT_EDGE_LABEL));
  }

  @Test
  public void createWithNullIDTest() {
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    assertThrows(NullPointerException.class, () -> {
      EdgeFactory.initEdge(null, sourceId, targetId);
    });
  }

  @Test
  public void createWithNullSourceIdTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    assertThrows(NullPointerException.class, () -> {
      EdgeFactory.initEdge(edgeId, null, targetId);
    });
  }

  @Test
  public void createWithNullTargetIdTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    assertThrows(NullPointerException.class, () -> {
      EdgeFactory.initEdge(edgeId, sourceId, null);
    });
  }

  @Test
  public void createWithNullLabelTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    assertThrows(NullPointerException.class, () -> {
      EdgeFactory.initEdge(edgeId, null, sourceId, targetId);
    });
  }

  @Test
  public void createInvertedEdgeTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    Edge e = EdgeFactory.initEdge(edgeId, sourceId, targetId);
    Edge invertedE = e.createReverseEdge();
    assertEquals(invertedE.getSourceId(), invertedE.getSourceId());
    assertEquals(invertedE.getTargetId(), invertedE.getTargetId());
    assertEquals(invertedE, invertedE);
  }
}

