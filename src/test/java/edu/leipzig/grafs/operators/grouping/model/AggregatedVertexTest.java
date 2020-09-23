package edu.leipzig.grafs.operators.grouping.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.leipzig.grafs.factory.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.junit.jupiter.api.Test;

class AggregatedVertexTest {

  @Test
  public void testIsAggregated_withVertex() {
    var v = VertexFactory.createVertex();
    var av = new AggregatedVertex();
    av.addVertex(v);
    assertTrue(av.isAlreadyAggregated(v));
  }

  @Test
  public void testIsAggregated_withId() {
    var id = GradoopId.get();
    var av = new AggregatedVertex();
    av.addVertex(id);
    assertTrue(av.isAlreadyAggregated(id));
  }

  @Test
  public void testIsAggregated_withMultipleIds() {
    var idSet = new GradoopIdSet();
    var id1 = GradoopId.get();
    var id2 = GradoopId.get();
    idSet.add(id1);
    idSet.add(id2);
    var av = new AggregatedVertex();
    av.addVertices(idSet);
    assertTrue(av.isAlreadyAggregated(id1));
    assertTrue(av.isAlreadyAggregated(id2));
    assertThat(av.getAggregatedVertexIds(), is(equalTo(idSet)));
    av.addVertices(idSet);
    assertThat(av.getAggregatedVertexIds(), is(equalTo(idSet)));

  }

}