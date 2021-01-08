package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class EdgeQueryFilterTest {

  static EdgeQueryFilter edgeFilter;

  @BeforeAll
  static void init() {
    var queryGraph = new Graph();
    var vertexA = new Vertex();
    vertexA.setLabel("A");
    var vertexB = new Vertex();
    vertexB.setLabel("B");
    queryGraph.addVertex(vertexA);
    queryGraph.addVertex(vertexB);
    var edge = EdgeFactory.createEdge(vertexA, vertexB);
    edge.setLabel("E");
    queryGraph.addEdge(edge);
    edgeFilter = new EdgeQueryFilter(queryGraph);
  }

  @Test
  void testFilter_sourceIsNotInQGraph() throws Exception {
    var source = new Vertex();
    source.setLabel("NotA");
    var target = new Vertex();
    target.setLabel("B");
    var edge = EdgeFactory.createEdge(source, target);
    var triplet = new Triplet(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_targetIsNotInQGraph() throws Exception {
    var source = new Vertex();
    source.setLabel("A");
    var target = new Vertex();
    target.setLabel("NotB");
    var edge = EdgeFactory.createEdge(source, target);
    var triplet = new Triplet(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_reverseEdgeIsInQGraph() throws Exception {
    var source = new Vertex();
    source.setLabel("B");
    var target = new Vertex();
    target.setLabel("A");
    var edge = EdgeFactory.createEdge(source, target);
    var triplet = new Triplet(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_SourceAndTargetAreInQGraphButEdgeIsNot() throws Exception {
    var source = new Vertex();
    source.setLabel("A");
    var target = new Vertex();
    target.setLabel("B");
    var edge = EdgeFactory.createEdge(source, target);
    var triplet = new Triplet(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_EdgeIsInQGraph() throws Exception {
    var source = new Vertex();
    source.setLabel("A");
    var target = new Vertex();
    target.setLabel("B");
    var edge = EdgeFactory.createEdge(source, target);
    edge.setLabel("E");
    var triplet = new Triplet(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(true)));
  }
}