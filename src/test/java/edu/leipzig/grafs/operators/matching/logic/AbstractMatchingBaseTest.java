package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.logic.AbstractMatchingBase.EdgeContainerFactory;
import edu.leipzig.grafs.operators.matching.model.CandidateMap;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import edu.leipzig.grafs.util.TestUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.jupiter.api.Test;

class AbstractMatchingBaseTest extends MatchingTestBase {


  @Test
  void testFeasibleVertexMatches() {
    AbstractMatchingBase<Window> matching = new MockedMatching(queryGraph);
    var matches = matching.feasibleVertexMatches(graph);

    var actualMatches = new CandidateMap<Vertex>();
    // A's
    var queryVertex = queryLoader.getVertexByVariable("qa1");
    var matchingVertexVariables = Set
        .of("v1", "v6", "v8", "v12", "v16", "v19", "v20", "v24", "v27", "v30");
    for (var vertexVar : matchingVertexVariables) {
      var matchingVertex = graphLoader.getVertexByVariable(vertexVar);
      actualMatches.addCandidate(queryVertex, matchingVertex);
    }

    // B's
    queryVertex = queryLoader.getVertexByVariable("qb2");
    matchingVertexVariables = Set
        .of("v2", "v7", "v13", "v15", "v17", "v21", "v23", "v26", "v29");
    for (var vertexVar : matchingVertexVariables) {
      var matchingVertex = graphLoader.getVertexByVariable(vertexVar);
      actualMatches.addCandidate(queryVertex, matchingVertex);
    }
    // C1's
    queryVertex = queryLoader.getVertexByVariable("qc3");
    matchingVertexVariables = Set
        .of("v3", "v4", "v5", "v9", "v11", "v14", "v18", "v22", "v25", "v28");
    for (var vertexVar : matchingVertexVariables) {
      var matchingVertex = graphLoader.getVertexByVariable(vertexVar);
      actualMatches.addCandidate(queryVertex, matchingVertex);
    }
    // C2's
    queryVertex = queryLoader.getVertexByVariable("qc4");
    for (var vertexVar : matchingVertexVariables) {
      var matchingVertex = graphLoader.getVertexByVariable(vertexVar);
      actualMatches.addCandidate(queryVertex, matchingVertex);
    }
    assertThat(actualMatches, equalTo(matches));

  }

  @Test
  void testBuildEdgeContainerSet() {
    AbstractMatchingBase<Window> matching = new MockedMatching(queryGraph);

    Set<Vertex> g1Set = new HashSet<>(graphLoader.getVerticesByVariables("v1", "v2", "v3", "v4"));
    Set<Vertex> g2Set = new HashSet<>(graphLoader.getVerticesByVariables("v1", "v2", "v3", "v5"));
    Set<Vertex> g3Set = new HashSet<>(graphLoader.getVerticesByVariables("v1", "v2", "v4", "v5"));
    var setOfSets = Set.of(g1Set, g2Set, g3Set);
    var actualEcCol = matching.buildEdgeContainerSet(setOfSets, graph);
    // TODO: build graph for every single on of them
    // think about the newly generated IDs (1 for each found pattern)
    var expectedEcSet = graphLoader.createEdgeContainersByGraphVariables("g1", "g2", "g3");
    TestUtils.validateEdgeContainerCollections(expectedEcSet, actualEcCol);
  }

  @Test
  void testNotAlreadyCandidate() {
    AbstractMatchingBase<Window> matching = new MockedMatching(queryGraph);
    var qV0 = new Vertex();
    var qV1 = new Vertex();
    var v1 = new Vertex();
    var v2 = new Vertex();
    var vNotCandidate = new Vertex();

    var candidates = new CandidateMap<Vertex>();
    candidates.addCandidate(qV0, v1);
    candidates.addCandidate(qV1, v2);
    candidates.addCandidate(qV1, v1);
    Vertex[] qVertexArray = {qV0, qV1};

    int depth = 0;
    assertThat(matching.notAlreadyCandidate(v1, candidates, qVertexArray, depth), is(true));
    assertThat(matching.notAlreadyCandidate(v2, candidates, qVertexArray, depth), is(true));
    assertThat(matching.notAlreadyCandidate(vNotCandidate, candidates, qVertexArray, depth),
        is(true));

    depth++;
    assertThat(matching.notAlreadyCandidate(v1, candidates, qVertexArray, depth), is(false));
    assertThat(matching.notAlreadyCandidate(v2, candidates, qVertexArray, depth), is(true));
    assertThat(matching.notAlreadyCandidate(vNotCandidate, candidates, qVertexArray, depth),
        is(true));

    depth++;
    assertThat(matching.notAlreadyCandidate(v1, candidates, qVertexArray, depth), is(false));
    assertThat(matching.notAlreadyCandidate(v2, candidates, qVertexArray, depth), is(false));
    assertThat(matching.notAlreadyCandidate(vNotCandidate, candidates, qVertexArray, depth),
        is(true));
  }

  @Test
  void testBuildPermutations() {
    AbstractMatchingBase<Window> matching = new MockedMatching(queryGraph);
    var v1 = new Vertex();
    var v2 = new Vertex();
    var v3 = new Vertex();
    var v4 = new Vertex();
    var v5 = new Vertex();
    var listOfCandidateSets = new ArrayList<>(
        List.of(
            Set.of(v1, v2),
            Set.of(v3),
            Set.of(v1, v4, v5)));

    var expectedPermutations = Set.of(
        Set.of(v1, v3), Set.of(v1, v3, v4), Set.of(v1, v3, v5),
        Set.of(v2, v3, v1), Set.of(v2, v3, v4), Set.of(v2, v3, v5));
    var actualPermutations = matching.buildPermutations(listOfCandidateSets);
    assertThat(actualPermutations, is(equalTo(expectedPermutations)));
  }

  @Test
  void testUpdatableEdgeContainerSet_testContains() {
    var uecSet = new EdgeContainerFactory();
    var v1 = graphLoader.getVertexByVariable("v1");
    var v2 = graphLoader.getVertexByVariable("v2");

    var e1 = graphLoader.getEdgeByVariable("e1");

    uecSet.add(e1, v1, v2, GradoopId.get());
    assertThat(uecSet.contains(new EdgeContainer(e1, v1, v2)), is(true));
  }

  @Test
  void testUpdatableEdgeContainerSet_sameEdgeUpdate() {
    var uecSet = new EdgeContainerFactory();
    var v1 = graphLoader.getVertexByVariable("v1");
    var v2 = graphLoader.getVertexByVariable("v2");

    var e1 = graphLoader.getEdgeByVariable("e1");

    var gId1 = GradoopId.get();
    var gId2 = GradoopId.get();

    uecSet.add(e1, v1, v2, gId1);
    uecSet.add(e1, v1, v2, gId2);
    assertThat(uecSet.getEdgeContainers(), hasSize(1));

    var ec = uecSet.getEdgeContainers().iterator().next();
    var actualE = ec.getEdge();
    assertThat(actualE.getGraphIds().contains(gId1), is(true));
    assertThat(actualE.getGraphIds().contains(gId2), is(true));

    var actualSource = ec.getSourceVertex();
    assertThat(actualSource.getGraphIds().contains(gId1), is(true));
    assertThat(actualSource.getGraphIds().contains(gId2), is(true));

    var actualTarget = ec.getTargetVertex();
    assertThat(actualTarget.getGraphIds().contains(gId1), is(true));
    assertThat(actualTarget.getGraphIds().contains(gId2), is(true));
  }

  @Test
  void testUpdatableEdgeContainerSet_differentEdgeUpdate() {
    var uecSet = new EdgeContainerFactory();
    var v1 = graphLoader.getVertexByVariable("v1");
    var v2 = graphLoader.getVertexByVariable("v2");
    var v3 = graphLoader.getVertexByVariable("v3");

    var e1 = graphLoader.getEdgeByVariable("e1");
    var e3 = graphLoader.getEdgeByVariable("e3");

    var gId1 = GradoopId.get();
    var gId2 = GradoopId.get();

    uecSet.add(e1, v1, v2, gId1);
    uecSet.add(e3, v2, v3, gId2);
    assertThat(uecSet.getEdgeContainers(), hasSize(2));

    var ecList = new ArrayList<>(uecSet.getEdgeContainers());
    ecList.sort(Comparator.comparing(ec -> ec.getSourceVertex().getLabel()));

    var ec = ecList.get(0);
    var actualE = ec.getEdge();
    assertThat(actualE.getGraphIds().contains(gId1), is(true));

    var actualSource = ec.getSourceVertex();
    assertThat(actualSource.getGraphIds().contains(gId1), is(true));

    var actualTarget = ec.getTargetVertex();
    assertThat(actualTarget.getGraphIds().contains(gId1), is(true));

    ec = ecList.get(1);
    actualE = ec.getEdge();
    assertThat(actualE.getGraphIds().contains(gId2), is(true));

    actualSource = ec.getSourceVertex();
    assertThat(actualSource.getGraphIds().contains(gId2), is(true));

    actualTarget = ec.getTargetVertex();
    assertThat(actualTarget.getGraphIds().contains(gId2), is(true));

    assertThat(ecList.get(0).getTargetVertex(), is(equalTo(ecList.get(1).getSourceVertex())));
  }


  static class MockedMatching extends AbstractMatchingBase<Window> {

    public MockedMatching(QueryGraph query) {
      super(query);
    }

    @Override
    void processQuery(Graph graph, Collector<EdgeContainer> collector) {

    }
  }
}