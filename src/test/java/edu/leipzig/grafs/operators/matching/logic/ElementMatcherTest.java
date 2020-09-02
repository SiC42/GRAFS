package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import edu.leipzig.grafs.model.GraphElement;
import edu.leipzig.grafs.model.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.junit.jupiter.api.Test;

class ElementMatcherTest {

  @Test
  void testMatchesQueryElem_nullProperties() {
    var label = GradoopConstants.DEFAULT_VERTEX_LABEL;
    GraphElement q = new Vertex();
    GraphElement v = new Vertex();

    assertThat(ElementMatcher.matchesQueryElem(q, v), is(true));
  }

  @Test
  void testMatchesQueryElem_givenLabel() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());

    assertThat(ElementMatcher.matchesQueryElem(q, v), is(true));
  }

  @Test
  void testMatchesQueryElem_differentLabels() {
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), "testlabel", Properties.create(),
        new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), "otherLabel", Properties.create(),
        new GradoopIdSet());

    assertThat(ElementMatcher.matchesQueryElem(q, v), is(false));
  }

  @Test
  void testMatchesQueryElem_defaultLabel() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL,
        Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());

    assertThat(ElementMatcher.matchesQueryElem(q, v), is(true));
  }

  @Test
  void testMatchesQueryElem_graphIdInsertion() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());

    q.addGraphId(graphId);
    assertThat(ElementMatcher.matchesQueryElem(q, v), is(true));
  }

  @Test
  void testMatchesQueryElem_PropertyManipulation() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());

    q.setProperty("1", 2);
    assertThat(ElementMatcher.matchesQueryElem(q, v), is(false));
    v.setProperty("1", 2);
    assertThat(ElementMatcher.matchesQueryElem(q, v), is(true));
  }

  @Test
  void testMatchesQueryElem_SameKeyDifferentValue() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());

    q.setProperty("1", 2);
    v.setProperty("1", 3);
    assertThat(ElementMatcher.matchesQueryElem(q, v), is(false));
  }

  @Test
  void testMatchesQueryElem_qElementIsMissingProp() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());

    v.setProperty("1", 3);
    assertThat(ElementMatcher.matchesQueryElem(q, v), is(true));
  }

  @Test
  void testMatchesQueryElem_elementIsMissingProp() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());

    q.setProperty("1", 3);
    assertThat(ElementMatcher.matchesQueryElem(q, v), is(false));
  }

  @Test
  void testMatchesQueryElem_qElementIsMissingLabel() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL,
        Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());

    assertThat(ElementMatcher.matchesQueryElem(q, v), is(true));
  }

  @Test
  void testMatchesQueryElem_elementIsMissingLabel() {
    var label = "testlabel";
    var graphId = GradoopId.get();
    GraphElement q = new Vertex(GradoopId.get(), label, Properties.create(), new GradoopIdSet());
    GraphElement v = new Vertex(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL,
        Properties.create(), new GradoopIdSet());

    assertThat(ElementMatcher.matchesQueryElem(q, v), is(false));
  }
}