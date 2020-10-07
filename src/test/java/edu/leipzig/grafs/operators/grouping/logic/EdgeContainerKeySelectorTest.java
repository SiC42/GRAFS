package edu.leipzig.grafs.operators.grouping.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.util.Collection;
import org.junit.jupiter.api.Test;

class EdgeContainerKeySelectorTest {

  private final EdgeContainer ec;

  public EdgeContainerKeySelectorTest() {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(
        "(a18:v {n : \"A\", a : 18})," +
            "(b17:v {n : \"B\", a : 17})," +
            "(a18)-[e1:e {t: 5, x: \"value\", a: 8}]->(b17)"
    );
    Collection<EdgeContainer> edgeSet = loader.createEdgeContainers();
    ec = edgeSet.iterator().next();

  }

  @Test
  void getKey_forSourceVertex() {
    GroupingInformation gi = new GroupingInformation();
    gi.addKey("n");
    EdgeContainerKeySelector eks = new EdgeContainerKeySelector(gi, null, AggregateMode.SOURCE);
    assertThat(eks.getKey(ec), equalTo("({n:A})-[]->()"));
  }

  @Test
  void getKey_testNumberProperty() {
    GroupingInformation gi = new GroupingInformation();
    gi.addKey("a");
    EdgeContainerKeySelector eks = new EdgeContainerKeySelector(gi, null, AggregateMode.SOURCE);
    assertThat(eks.getKey(ec), equalTo("({a:18})-[]->()"));
  }

  @Test
  void getKey_testLabelForVertex() {
    GroupingInformation gi = new GroupingInformation();
    gi.useLabel(true);
    EdgeContainerKeySelector eks = new EdgeContainerKeySelector(gi, null, AggregateMode.SOURCE);
    assertThat(eks.getKey(ec), equalTo("(:v)-[]->()"));
  }

  @Test
  void getKey_forTargetVertex() {
    GroupingInformation gi = new GroupingInformation();
    gi.addKey("n");
    EdgeContainerKeySelector eks = new EdgeContainerKeySelector(gi, null, AggregateMode.TARGET);
    assertThat(eks.getKey(ec), equalTo("()-[]->({n:B})"));
  }

  @Test
  void getKey_forEdgeWithVertexGroupingInformation() {
    GroupingInformation vgi = new GroupingInformation();
    vgi.addKey("n");
    GroupingInformation egi = new GroupingInformation();
    egi.addKey("t");
    EdgeContainerKeySelector eks = new EdgeContainerKeySelector(vgi, egi, AggregateMode.EDGE);
    assertThat(eks.getKey(ec), equalTo("({n:A})-[{t:5}]->({n:B})"));
  }

  @Test
  void getKey_testLabelForEdge() {
    GroupingInformation vgi = new GroupingInformation();
    GroupingInformation egi = new GroupingInformation();
    egi.useLabel(true);
    EdgeContainerKeySelector eks = new EdgeContainerKeySelector(vgi, egi, AggregateMode.EDGE);
    assertThat(eks.getKey(ec), equalTo("()-[:e]->()"));
  }

  @Test
  void getKey_forEdgeWithoutEdgeGroupingInformation() {
    GroupingInformation vgi = new GroupingInformation();
    EdgeContainerKeySelector eks = new EdgeContainerKeySelector(vgi, null, AggregateMode.EDGE);
    assertThat(eks.getKey(ec), equalTo("()-[:e {a:8,t:5,x:value}]->()"));
  }
}