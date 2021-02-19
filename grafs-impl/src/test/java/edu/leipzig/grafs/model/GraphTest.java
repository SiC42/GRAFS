package edu.leipzig.grafs.model;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.util.TestUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class GraphTest {

  @Test
  void testFromTriplets() throws IOException {
    var loader = TestUtils.getSocialNetworkLoader();
    var triplets = loader.createTriplets();
    var actualGraph = Graph.fromTriplets(triplets);
    var expectedGraph = loader.createGraph();
    TestUtils.validateElementCollections(actualGraph.getVertices(), expectedGraph.getVertices());
    TestUtils.validateElementCollections(actualGraph.getEdges(), expectedGraph.getEdges());
  }


  @Test
  void testAddVertex() {
    var graph = new Graph<>();
    var vertex = new Vertex();
    graph.addVertex(vertex);
    assertThat(graph.getVertices(), hasSize(1));
    assertThat(graph.getVertices(), hasItem(vertex));
  }

  @Test
  void testAddVertex_AddTwoTimes() {
    var graph = new Graph<>();
    var vertex = new Vertex();
    graph.addVertex(vertex);
    graph.addVertex(vertex);
    assertThat(graph.getVertices(), hasSize(1));
    assertThat(graph.getVertices(), hasItem(vertex));
  }

  @Test
  void testAddVertices() {
    var graph = new Graph<>();
    var vertex = new Vertex();
    var vertexSet = Set.of(vertex);
    graph.addVertices(vertexSet);
    assertThat(graph.getVertices(), hasSize(1));
    assertThat(graph.getVertices(), hasItem(vertex));
  }

  @Test
  void testAddEdge() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target = new Vertex();
    var edge = EdgeFactory.createEdge(source.getId(), target.getId());
    graph.addVertex(source);
    graph.addVertex(target);
    graph.addEdge(edge);
    assertThat(graph.getEdges(), hasSize(1));
    assertThat(graph.getEdges(), hasItem(edge));
  }

  @Test
  void testAddEdge_noSource() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target = new Vertex();
    var edge = EdgeFactory.createEdge(source.getId(), target.getId());
    graph.addVertex(target);
    assertThrows(RuntimeException.class, () -> graph.addEdge(edge));
  }

  @Test
  void testAddEdge_noTarget() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target = new Vertex();
    var edge = EdgeFactory.createEdge(source.getId(), target.getId());
    graph.addVertex(source);
    assertThrows(RuntimeException.class, () -> graph.addEdge(edge));
  }

  @Test
  void testAddEdges() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target = new Vertex();
    var edge = EdgeFactory.createEdge(source.getId(), target.getId());
    graph.addVertex(source);
    graph.addVertex(target);
    graph.addEdges(Set.of(edge));
    assertThat(graph.getEdges(), hasSize(1));
    assertThat(graph.getEdges(), hasItem(edge));
  }

  @Test
  void testGetSourceForEdge() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target = new Vertex();
    var edge = EdgeFactory.createEdge(source.getId(), target.getId());
    graph.addVertex(source);
    graph.addVertex(target);
    graph.addEdge(edge);
    assertThat(graph.getSourceForEdge(edge), is(equalTo(source)));
  }

  @Test
  void testGetTargetForEdge() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target = new Vertex();
    var edge = EdgeFactory.createEdge(source.getId(), target.getId());
    graph.addVertex(source);
    graph.addVertex(target);
    graph.addEdge(edge);
    assertThat(graph.getTargetForEdge(edge), is(equalTo(target)));
  }

  @Test
  void testGetEdgesForSource() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target1 = new Vertex();
    var target2 = new Vertex();
    var edge1 = EdgeFactory.createEdge(source.getId(), target1.getId());
    var edge2 = EdgeFactory.createEdge(source.getId(), target2.getId());
    graph.addVertex(source);
    graph.addVertex(target1);
    graph.addVertex(target2);
    graph.addEdge(edge1);
    graph.addEdge(edge2);
    assertThat(graph.getEdgesForSource(source), hasSize(2));
    assertThat(graph.getEdgesForSource(source), hasItem(edge1));
    assertThat(graph.getEdgesForSource(source), hasItem(edge2));
  }

  @Test
  void testGetEdgesForSource_NoEdges() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target1 = new Vertex();
    graph.addVertex(source);
    graph.addVertex(target1);
    assertThat(graph.getEdgesForSource(source), is(equalTo(Collections.EMPTY_SET)));
  }

  @Test
  void testGetEdgesForTarget() {
    var graph = new Graph<>();
    var source1 = new Vertex();
    var source2 = new Vertex();
    var target = new Vertex();
    var edge1 = EdgeFactory.createEdge(source1.getId(), target.getId());
    var edge2 = EdgeFactory.createEdge(source2.getId(), target.getId());
    graph.addVertex(source1);
    graph.addVertex(source2);
    graph.addVertex(target);
    graph.addEdge(edge1);
    graph.addEdge(edge2);
    assertThat(graph.getEdgesForTarget(target), hasSize(2));
    assertThat(graph.getEdgesForTarget(target), hasItem(edge1));
    assertThat(graph.getEdgesForTarget(target), hasItem(edge2));
  }

  @Test
  void testGetEdgesForTarget_NoEdges() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target = new Vertex();
    graph.addVertex(source);
    graph.addVertex(target);
    assertThat(graph.getEdgesForTarget(target), is(equalTo(Collections.EMPTY_SET)));
  }

  @Test
  void testGetTargetForSourceVertex() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target1 = new Vertex();
    var target2 = new Vertex();
    var edge1 = EdgeFactory.createEdge(source.getId(), target1.getId());
    var edge2 = EdgeFactory.createEdge(source.getId(), target2.getId());
    graph.addVertex(source);
    graph.addVertex(target1);
    graph.addVertex(target2);
    graph.addEdge(edge1);
    graph.addEdge(edge2);
    assertThat(graph.getTargetForSourceVertex(source), hasSize(2));
    assertThat(graph.getTargetForSourceVertex(source), hasItem(target1));
    assertThat(graph.getTargetForSourceVertex(source), hasItem(target2));
  }

  @Test
  void testGetTargetForSourceVertex_NoEdges() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target1 = new Vertex();
    graph.addVertex(source);
    graph.addVertex(target1);
    assertThat(graph.getTargetForSourceVertex(source), is(equalTo(Collections.EMPTY_SET)));
  }

  @Test
  void testGetEdgeForVertices() {
    var graph = new Graph<>();
    var source = new Vertex();
    var target1 = new Vertex();
    var target2 = new Vertex();
    var edge1 = EdgeFactory.createEdge(source.getId(), target1.getId());
    var edge2 = EdgeFactory.createEdge(source.getId(), target2.getId());
    graph.addVertex(source);
    graph.addVertex(target1);
    graph.addVertex(target2);
    graph.addEdge(edge1);
    graph.addEdge(edge2);
    assertThat(graph.getEdgesForVertices(source, target1).iterator().next(), is(equalTo(edge1)));
    assertThat(graph.getEdgesForVertices(source, target2).iterator().next(), is(equalTo(edge2)));
  }

  @Test
  void getVertexInducedSubGraph() throws IOException {
    var loader = TestUtils.getSocialNetworkLoader();
    var graph = loader.createGraph();
    var expectedSubGraph = loader.createGraphByGraphVariables("g0");

    var vertexSet = loader.getVerticesByGraphVariables("g0");
    var actualSubGraph = graph.getVertexInducedSubGraph(vertexSet);

    TestUtils
        .validateElementCollections(actualSubGraph.getVertices(), expectedSubGraph.getVertices());
    TestUtils.validateElementCollections(actualSubGraph.getEdges(), expectedSubGraph.getEdges());
  }

  @Test
  void testSerialization() throws IOException, ClassNotFoundException {
    var loader = TestUtils.getSocialNetworkLoader();
    var graph = loader.createGraph();
    byte[] serializedGraph = TestUtils.pickle(graph);
    var deserializedGraph = TestUtils.unpickle(serializedGraph, Graph.class);
    assertThat(deserializedGraph, is(equalTo(graph)));
  }
}