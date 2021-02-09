package edu.leipzig.grafs.operators.grouping.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.util.Collection;
import org.junit.jupiter.api.Test;

class TripletKeySelectorTest {

  private final Triplet<Vertex, Edge> triplet;

  public TripletKeySelectorTest() {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(
        "(a18:v {n : \"A\", a : 18})," +
            "(b17:v {n : \"B\", a : 17})," +
            "(a18)-[e1:e {t: 5, x: \"value\", a: 8}]->(b17)"
    );
    Collection<Triplet<Vertex, Edge>> edgeSet = loader.createTriplets();
    triplet = edgeSet.iterator().next();

  }

  @Test
  void getKey_forSourceVertex() {
    GroupingInformation gi = new GroupingInformation();
    gi.addKey("n");
    TripletKeySelector tks = new TripletKeySelector(gi, AggregateMode.SOURCE);
    assertThat(tks.getKey(triplet), equalTo("({n:A})-[]->()"));
  }

  @Test
  void getKey_testNumberProperty() {
    GroupingInformation gi = new GroupingInformation();
    gi.addKey("a");
    TripletKeySelector tks = new TripletKeySelector(gi, AggregateMode.SOURCE);
    assertThat(tks.getKey(triplet), equalTo("({a:18})-[]->()"));
  }

  @Test
  void getKey_testLabelForVertex() {
    GroupingInformation gi = new GroupingInformation();
    gi.useLabel(true);
    TripletKeySelector tks = new TripletKeySelector(gi, AggregateMode.SOURCE);
    assertThat(tks.getKey(triplet), equalTo("(:v)-[]->()"));
  }

  @Test
  void getKey_forTargetVertex() {
    GroupingInformation gi = new GroupingInformation();
    gi.addKey("n");
    TripletKeySelector tks = new TripletKeySelector(gi, AggregateMode.TARGET);
    assertThat(tks.getKey(triplet), equalTo("()-[]->({n:B})"));
  }

  @Test
  void getKey_forEdgeWithVertexGroupingInformation() {
    GroupingInformation egi = new GroupingInformation();
    egi.addKey("t");
    TripletKeySelector tks = new TripletKeySelector(egi, AggregateMode.EDGE);
    var expectedSource = triplet.getSourceVertex().getId();
    var expectedTarget = triplet.getTargetVertex().getId();
    assertThat(tks.getKey(triplet), equalTo(String.format("(%s)-[{t:5}]->(%s)",expectedSource, expectedTarget)));
  }

  @Test
  void getKey_testLabelForEdge() {
    GroupingInformation vgi = new GroupingInformation();
    GroupingInformation egi = new GroupingInformation();
    egi.useLabel(true);
    TripletKeySelector tks = new TripletKeySelector(egi, AggregateMode.EDGE);
    var expectedSource = triplet.getSourceVertex().getId();
    var expectedTarget = triplet.getTargetVertex().getId();
    assertThat(tks.getKey(triplet), equalTo(String.format("(%s)-[:e]->(%s)",expectedSource, expectedTarget)));
  }

  @Test
  void getKey_forEdgeWithoutEdgeGroupingInformation() {
    GroupingInformation vgi = new GroupingInformation();
    TripletKeySelector tks = new TripletKeySelector(vgi, AggregateMode.EDGE);
    var expectedSource = triplet.getSourceVertex().getId();
    var expectedTarget = triplet.getTargetVertex().getId();
    assertThat(tks.getKey(triplet), equalTo(String.format("(%s)-[]->(%s)",expectedSource, expectedTarget)));
  }
}