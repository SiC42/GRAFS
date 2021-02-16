package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.matching.model.Query;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class VertexQueryFilterTest {

  static VertexQueryFilter vertexFilter;

  @BeforeAll
  static void init() {
    var queryGraph = new Query();
    var vertexA = new QueryVertex();
    vertexA.setLabel("A");
    vertexA.addVariable("a");
    var vertexB = new QueryVertex();
    vertexB.setLabel("B");
    vertexB.addVariable("b");
    queryGraph.addVertex(vertexA);
    queryGraph.addVertex(vertexB);
    vertexFilter = new VertexQueryFilter(queryGraph);
  }

  @Test
  void testFilter_sourceIsNotInQGraph() throws Exception {
    var source = new QueryVertex();
    source.setLabel("NotA");
    var target = new QueryVertex();
    target.setLabel("B");
    var edge = EdgeFactory.createEdge(source, target);
    var triplet = new Triplet<>(edge, source, target);
    assertThat(vertexFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_targetIsNotInQGraph() throws Exception {
    var source = new QueryVertex();
    source.setLabel("A");
    var target = new QueryVertex();
    target.setLabel("NotB");
    var edge = EdgeFactory.createEdge(source, target);
    var triplet = new Triplet<>(edge, source, target);
    assertThat(vertexFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_EcIsInQGraph() throws Exception {
    var source = new QueryVertex();
    source.setLabel("A");
    var target = new QueryVertex();
    target.setLabel("B");
    var edge = EdgeFactory.createEdge(source, target);
    var triplet = new Triplet<>(edge, source, target);
    assertThat(vertexFilter.filter(triplet), is(equalTo(true)));
  }

}