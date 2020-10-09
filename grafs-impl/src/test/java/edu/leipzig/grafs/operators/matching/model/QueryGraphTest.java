package edu.leipzig.grafs.operators.matching.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.GraphTest;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.util.TestUtils;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class QueryGraphTest extends GraphTest {

  @Test
  void testFromGraph() throws IOException {
    var loader = TestUtils.getSocialNetworkLoader();
    var graph = loader.createGraph();
    var qGraph = QueryGraph.fromGraph(graph);
    assertThat(qGraph.getVertices(), is(equalTo(graph.getVertices())));
    assertThat(qGraph.getEdges(), is(equalTo(graph.getEdges())));
  }

  @Test
  void testIsVertexOnly() {
    var qGraph = new QueryGraph();
    assertThat(qGraph.isVertexOnly(), is(true));
    var v = new Vertex();
    qGraph.addVertex(v);
    assertThat(qGraph.isVertexOnly(), is(true));
    var ef = new EdgeFactory();
    var e = EdgeFactory.createEdge(v, v);
    qGraph.addEdge(e);
    assertThat(qGraph.isVertexOnly(), is(false));
  }
}