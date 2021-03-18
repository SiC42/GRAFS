package edu.leipzig.grafs.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.operators.grouping.model.ReversibleEdge;
import org.junit.jupiter.api.Test;

class TripletTest {

  @Test
  void testEdgeContainer_RightExecution() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var e = EdgeFactory.createEdge(v1, v2);

    assertDoesNotThrow(() -> new Triplet<>(e, v1, v2));
  }

  @Test
  void testEdgeContainer_wrongTarget() {
    var v1 = new Vertex();
    var v2 = new Vertex();
    var v3 = new Vertex();

    var e = EdgeFactory.createEdge(v1, v2);

    assertThrows(RuntimeException.class, () -> new Triplet<>(e, v1, v3));
  }

  @Test
  void testEdgeContainer_wrongSource() {
    var v1 = new Vertex();
    var v2 = new Vertex();
    var v3 = new Vertex();

    var e = EdgeFactory.createEdge(v1, v2);

    assertThrows(RuntimeException.class, () -> new Triplet<>(e, v3, v2));
  }


  @Test
  void testGetEdge() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var e = EdgeFactory.createEdge(v1, v2);
    var triplet = new Triplet<>(e, v1, v2);
    assertThat(triplet.getEdge(), is(equalTo(e)));
  }

  @Test
  void testGetSourceVertex() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var e = EdgeFactory.createEdge(v1, v2);
    var triplet = new Triplet<>(e, v1, v2);
    assertThat(triplet.getSourceVertex(), is(equalTo(v1)));
  }


  @Test
  void testGetTargetVertex() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var e = EdgeFactory.createEdge(v1, v2);
    var triplet = new Triplet<>(e, v1, v2);
    assertThat(triplet.getTargetVertex(), is(equalTo(v2)));
  }


  @Test
  void testCreateReverseEdgeContainer() {
    var v1 = new Vertex();
    var v2 = new Vertex();

    var e = EdgeFactory.createEdge(v1, v2);
    var reverseE = ReversibleEdge.create(e, true);
    var triplet = new Triplet<>(e, v1, v2);
    var revTriplet = triplet.createReverseTriplet();
    assertThat(revTriplet.getEdge(), is(equalTo(reverseE)));
  }
}