package edu.leipzig.grafs.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.leipzig.grafs.factory.EdgeFactory;
import org.junit.jupiter.api.Test;

class EdgeContainerTest {

  @Test
  void testEdgeContainer_RightExecution() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var ef = new EdgeFactory();
    var e = ef.createEdge(v1, v2);

    assertDoesNotThrow(() -> new EdgeContainer(e, v1, v2));
  }

  @Test
  void testEdgeContainer_wrongTarget() {
    var v1 = new Vertex();
    var v2 = new Vertex();
    var v3 = new Vertex();

    var ef = new EdgeFactory();
    var e = ef.createEdge(v1, v2);

    assertThrows(RuntimeException.class, () -> new EdgeContainer(e, v1, v3));
  }

  @Test
  void testEdgeContainer_wrongSource() {
    var v1 = new Vertex();
    var v2 = new Vertex();
    var v3 = new Vertex();

    var ef = new EdgeFactory();
    var e = ef.createEdge(v1, v2);

    assertThrows(RuntimeException.class, () -> new EdgeContainer(e, v3, v2));
  }


  @Test
  void testGetEdge() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var ef = new EdgeFactory();
    var e = ef.createEdge(v1, v2);
    var ec = new EdgeContainer(e, v1, v2);
    assertThat(ec.getEdge(), is(equalTo(e)));
  }

  @Test
  void testGetSourceVertex() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var ef = new EdgeFactory();
    var e = ef.createEdge(v1, v2);
    var ec = new EdgeContainer(e, v1, v2);
    assertThat(ec.getSourceVertex(), is(equalTo(v1)));
  }


  @Test
  void testGetTargetVertex() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var ef = new EdgeFactory();
    var e = ef.createEdge(v1, v2);
    var ec = new EdgeContainer(e, v1, v2);
    assertThat(ec.getTargetVertex(), is(equalTo(v2)));
  }


  @Test
  void testCreateReverseEdgeContainer() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var ef = new EdgeFactory();
    var e = ef.createEdge(v1, v2);
    var reverseE = e.createReverseEdge();
    var ec = new EdgeContainer(e, v1, v2);
    ec = ec.createReverseEdgeContainer();
    assertThat(ec.getEdge(), is(equalTo(reverseE)));
  }
}