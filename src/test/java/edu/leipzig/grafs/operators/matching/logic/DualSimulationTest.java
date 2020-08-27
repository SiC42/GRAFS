package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class DualSimulationTest extends MatchingTestBase {

  @Test
  public void testBuildCandidates() {

    var ds = new DualSimulation<>(queryGraph);
    var candidateMap = ds.feasibleVertexMatches(graph);

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
  public void testCreateDualSimulationCandidates() {

    var ds = new DualSimulation<>(queryGraph);
    var candidateMap = ds.feasibleVertexMatches(graph);
    var candidates = DualSimulation.runAlgorithm(graph, queryGraph, candidateMap);

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
  public void testdualSimulationProcess() {
    var ds = new DualSimulation<>(queryGraph);
    var actualGraphs = ds.dualSimulationProcess(graph);

    var vars = new String[]{"g1", "g2", "g3"};
    Set<Graph> expectedGraphs = new HashSet<>();
    for (var i : vars) {
      var expectedGraph = graphLoader.createGraphByGraphVariables(i);
      expectedGraphs.add(expectedGraph);
    }

    //assertThat(actualGraphs, containsInAnyOrder(expectedGraphs.toArray()));
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

  private Set<Vertex> filterBySelfAssignedId(Collection<Vertex> vertices, int... ids) {
    Set<Vertex> filteredVertex = new HashSet<>();
    for (var vertex : vertices) {
      for (var id : ids) {
        if (vertex.getPropertyValue("id").toString().equals(Integer.toString(id))) {
          filteredVertex.add(vertex);
          break;
        }
      }
    }
    return filteredVertex;
  }

}