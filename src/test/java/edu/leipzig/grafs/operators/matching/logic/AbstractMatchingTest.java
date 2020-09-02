package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.logic.AbstractMatching.EdgeContainerFactory;
import edu.leipzig.grafs.operators.matching.model.CandidateMap;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import edu.leipzig.grafs.util.TestUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.jupiter.api.Test;

class AbstractMatchingTest extends MatchingTestBase {


  @Test
  public void testFeasibleVertexMatches() {
    AbstractMatching<Window> matching = new MockedMatching(queryGraph);
    var candidateMap = matching.feasibleVertexMatches(graph);

    var graphVertices = graphLoader.getVertices();
    var expectedCandidates = filterByLabel(graphVertices, "A");
    var currentQueryVertex = queryLoader.getVertexByVariable("qa1");
    var currentCandidates = candidateMap.getCandidatesFor(currentQueryVertex);
    assertThat(currentCandidates, containsInAnyOrder(expectedCandidates.toArray()));

    expectedCandidates = filterByLabel(graphVertices, "B");
    currentQueryVertex = queryLoader.getVertexByVariable("qb2");
    currentCandidates = candidateMap.getCandidatesFor(currentQueryVertex);
    assertThat(currentCandidates, containsInAnyOrder(expectedCandidates.toArray()));

    expectedCandidates = filterByLabel(graphVertices, "C");
    currentQueryVertex = queryLoader.getVertexByVariable("qc3");
    currentCandidates = candidateMap.getCandidatesFor(currentQueryVertex);
    assertThat(currentCandidates, containsInAnyOrder(expectedCandidates.toArray()));

    expectedCandidates = filterByLabel(graphVertices, "C");
    currentQueryVertex = queryLoader.getVertexByVariable("qc4");
    currentCandidates = candidateMap.getCandidatesFor(currentQueryVertex);
    assertThat(currentCandidates, containsInAnyOrder(expectedCandidates.toArray()));

  }

  @Test
  void testBuildEdgeContainerSet() {
    AbstractMatching<Window> matching = new MockedMatching(queryGraph);

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
    AbstractMatching<Window> matching = new MockedMatching(queryGraph);
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
    assertThat(matching.previouslyNotCandidate(v1, candidates, qVertexArray, depth), is(true));
    assertThat(matching.previouslyNotCandidate(v2, candidates, qVertexArray, depth), is(true));
    assertThat(matching.previouslyNotCandidate(vNotCandidate, candidates, qVertexArray, depth),
        is(true));

    depth++;
    assertThat(matching.previouslyNotCandidate(v1, candidates, qVertexArray, depth), is(false));
    assertThat(matching.previouslyNotCandidate(v2, candidates, qVertexArray, depth), is(true));
    assertThat(matching.previouslyNotCandidate(vNotCandidate, candidates, qVertexArray, depth),
        is(true));

    depth++;
    assertThat(matching.previouslyNotCandidate(v1, candidates, qVertexArray, depth), is(false));
    assertThat(matching.previouslyNotCandidate(v2, candidates, qVertexArray, depth), is(false));
    assertThat(matching.previouslyNotCandidate(vNotCandidate, candidates, qVertexArray, depth),
        is(true));
  }

  @Test
  void testBuildPermutations() {
    AbstractMatching<Window> matching = new MockedMatching(queryGraph);
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
  void testEdgeContainerFactory_testContains() {
    var uecSet = new EdgeContainerFactory();
    var v1 = graphLoader.getVertexByVariable("v1");
    var v2 = graphLoader.getVertexByVariable("v2");

    var e1 = graphLoader.getEdgeByVariable("e1");

    uecSet.add(e1, v1, v2, GradoopId.get());
    assertThat(uecSet.contains(new EdgeContainer(e1, v1, v2)), is(true));
  }

  @Test
  void testEdgeContainerFactory_sameEdgeUpdate() {
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
  void testEdgeContainerFactory_differentEdgeUpdate() {
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

  private Set<Vertex> filterByLabel(Collection<Vertex> vertices, String... labels) {
    Set<Vertex> filteredVertex = new HashSet<>();
    for (var vertex : vertices) {
      for (String label : labels) {
        if (vertex.getLabel().equals(label)) {
          filteredVertex.add(vertex);
          break;
        }
      }
    }
    return filteredVertex;
  }


  static class MockedMatching extends AbstractMatching<Window> {

    public MockedMatching(QueryGraph query) {
      super(query);
    }

    @Override
    void processQuery(Graph graph, Collector<EdgeContainer> collector) {

    }
  }
}