package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;

import edu.leipzig.grafs.operators.DummyCollector;
import edu.leipzig.grafs.operators.matching.model.CandidateMap;
import org.junit.jupiter.api.Test;

class DualSimulationProcessTest extends MatchingTestBase {


  @Test
  public void testRunAlgorithm() {

    var ds = new DualSimulationProcess<>(queryGraph);
    var candidateMap = ds.feasibleVertexMatches(graph);
    var candidates = DualSimulationProcess.runAlgorithm(graph, queryGraph, candidateMap);

    var graphVertices = graphLoader.getVertices();
    var expectedCandidates = filterBySelfAssignedId(graphVertices, 1, 6, 8, 12, 16, 19, 20, 24, 27,
        30);
    var currentQueryVertex = queryLoader.getVertexByVariable("qa1");
    var currentCandidates = candidateMap.getCandidatesFor(currentQueryVertex);
    assertThat(currentCandidates, containsInAnyOrder(expectedCandidates.toArray()));

    expectedCandidates = filterBySelfAssignedId(graphVertices, 2, 7, 13, 15, 17, 21, 23, 26, 29);
    currentQueryVertex = queryLoader.getVertexByVariable("qb2");
    currentCandidates = candidateMap.getCandidatesFor(currentQueryVertex);
    assertThat(currentCandidates, containsInAnyOrder(expectedCandidates.toArray()));

    expectedCandidates = filterBySelfAssignedId(graphVertices, 3, 4, 5, 9, 14, 18, 22, 25, 28);
    currentQueryVertex = queryLoader.getVertexByVariable("qc3");
    currentCandidates = candidateMap.getCandidatesFor(currentQueryVertex);
    assertThat(currentCandidates, containsInAnyOrder(expectedCandidates.toArray()));

    expectedCandidates = filterBySelfAssignedId(graphVertices, 3, 4, 5, 9, 14, 18, 22, 25, 28);
    currentQueryVertex = queryLoader.getVertexByVariable("qc4");
    currentCandidates = candidateMap.getCandidatesFor(currentQueryVertex);
    assertThat(currentCandidates, containsInAnyOrder(expectedCandidates.toArray()));
  }

  @Test
  public void testRunAlgorithm_withEmptyCandidates() {
    var candidates = DualSimulationProcess.runAlgorithm(graph, queryGraph, new CandidateMap<>());
    assertThat(candidates.size(), is(0));

  }

  @Test
  public void testProcessQuery() {
    var appendDsGraph = "ds {}["
        // ds-edges
        // from blue
        + "(v1)-[e1]->(v2)"
        + "(v2)-[e2]->(v1)"
        + "(v2)-[e3]->(v3)"
        + "(v2)-[e4]->(v4)"
        + "(v2)-[e5]->(v5)"
        + "(v2)-[e6]->(v6)"
        // from green
        + "(v6)-[e7]->(v7)"
        + "(v7)-[e9]->(v8)"
        + "(v7)-[e10]->(v5)"
        + "(v7)-[e11]->(v9)"
        + "(v8)-[e13]->(v2)"
        // from pink
        + "(v12)-[e15]->(v13)"
        + "(v13)-[e16]->(v12)"
        + "(v13)-[e17]->(v14)"
        // from yellow
        + "(v15)-[e18]->(v14)"
        + "(v15)-[e19]->(v16)"
        + "(v16)-[e20]->(v17)"
        + "(v17)-[e21]->(v18)"
        + "(v17)-[e22]->(v19)"
        + "(v19)-[e25]->(v21)"
        + "(v20)-[e26]->(v15)"
        + "(v21)-[e27]->(v20)"
        + "(v21)-[e28]->(v22)"
        // from grey
        + "(v23)-[e29]->(v22)"
        + "(v23)-[e30]->(v24)"
        + "(v24)-[e31]->(v26)"
        + "(v24)-[e31]->(v27)"
        + "(v26)-[e32]->(v25)"
        + "(v26)-[e33]->(v27)"
        + "(v27)-[e34]->(v29)"
        + "(v29)-[e35]->(v28)"
        + "(v29)-[e36]->(v30)"
        + "(v30)-[e37]->(v23)]";
    graphLoader.appendFromString(appendDsGraph);
    var ds = new DualSimulationProcess<>(queryGraph);
    var collector = new DummyCollector();
    ds.processQuery(graph, collector);
    var actualEcs = collector.getCollected();
    var expectedEcs = graphLoader.createEdgeContainersByGraphVariables("ds");
    assertThat(actualEcs, containsInAnyOrder(expectedEcs.toArray()));
  }


}