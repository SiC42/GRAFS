package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

import edu.leipzig.grafs.model.BasicGraph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.GradoopConstants;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class EdgeQueryFilterTest {

  static EdgeQueryFilter edgeFilter;

  @BeforeAll
  static void init() {
    var queryGraph = new BasicGraph<QueryVertex,QueryEdge>();
    var vertexA = new QueryVertex();
    vertexA.setLabel("A");
    var vertexB = new QueryVertex();
    vertexB.setLabel("B");
    queryGraph.addVertex(vertexA);
    queryGraph.addVertex(vertexB);
    var edge = new QueryEdge(GradoopId.get(), "E", vertexA.getId(),  vertexB.getId(), null,  new GradoopIdSet());
    queryGraph.addEdge(edge);
    edgeFilter = new EdgeQueryFilter(queryGraph);
  }

  @Test
  void testFilter_sourceIsNotInQGraph() throws Exception {
    var source = new QueryVertex();
    source.setLabel("NotA");
    var target = new QueryVertex();
    target.setLabel("B");
    var edge = new QueryEdge(GradoopId.get(), GradoopConstants.DEFAULT_EDGE_LABEL, source.getId(),  target.getId(), null,  new GradoopIdSet());
    var triplet = new Triplet<>(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_targetIsNotInQGraph() throws Exception {
    var source = new QueryVertex();
    source.setLabel("A");
    var target = new QueryVertex();
    target.setLabel("NotB");
    var edge = new QueryEdge(GradoopId.get(), GradoopConstants.DEFAULT_EDGE_LABEL, source.getId(),  target.getId(), null,  new GradoopIdSet());
    var triplet = new Triplet<>(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_reverseEdgeIsInQGraph() throws Exception {
    var source = new QueryVertex();
    source.setLabel("B");
    var target = new QueryVertex();
    target.setLabel("A");
    var edge = new QueryEdge(GradoopId.get(), GradoopConstants.DEFAULT_EDGE_LABEL, source.getId(),  target.getId(), null,  new GradoopIdSet());
    var triplet = new Triplet<>(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_SourceAndTargetAreInQGraphButEdgeIsNot() throws Exception {
    var source = new QueryVertex();
    source.setLabel("A");
    var target = new QueryVertex();
    target.setLabel("B");
    var edge = new QueryEdge(GradoopId.get(), GradoopConstants.DEFAULT_EDGE_LABEL, source.getId(),  target.getId(), null,  new GradoopIdSet());
    var triplet = new Triplet<>(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(false)));
  }

  @Test
  void testFilter_EdgeIsInQGraph() throws Exception {
    var source = new QueryVertex();
    source.setLabel("A");
    var target = new QueryVertex();
    target.setLabel("B");
    var edge = new QueryEdge(GradoopId.get(), "E", source.getId(),  target.getId(), null,  new GradoopIdSet());
    var triplet = new Triplet<>(edge, source, target);
    assertThat(edgeFilter.filter(triplet), is(equalTo(true)));
  }
}