package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class VertexQueryFilterTest {
  static VertexQueryFilter vertexFilter;

  @BeforeAll
  static void init(){
    var queryGraph = new QueryGraph();
    var vertexA = new Vertex();
    vertexA.setLabel("A");
    var vertexB = new Vertex();
    vertexB.setLabel("B");
    queryGraph.addVertex(vertexA);
    queryGraph.addVertex(vertexB);
    vertexFilter = new VertexQueryFilter(queryGraph);
  }

  @Test
  void testFilter_sourceIsNotInQGraph() throws Exception {
    var source = new Vertex();
    source.setLabel("NotA");
    var target = new Vertex();
    target.setLabel("B");
    var edge = EdgeFactory.createEdge(source, target);
    var ec = new EdgeContainer(edge, source, target);
    assertThat(vertexFilter.filter(ec), is(equalTo(false)));
  }

  @Test
  void testFilter_targetIsNotInQGraph() throws Exception {
    var source = new Vertex();
    source.setLabel("A");
    var target = new Vertex();
    target.setLabel("NotB");
    var edge = EdgeFactory.createEdge(source, target);
    var ec = new EdgeContainer(edge, source, target);
    assertThat(vertexFilter.filter(ec), is(equalTo(false)));
  }

  @Test
  void testFilter_EcIsInQGraph() throws Exception {
    var source = new Vertex();
    source.setLabel("A");
    var target = new Vertex();
    target.setLabel("B");
    var edge = EdgeFactory.createEdge(source, target);
    var ec = new EdgeContainer(edge, source, target);
    assertThat(vertexFilter.filter(ec), is(equalTo(true)));
  }

}