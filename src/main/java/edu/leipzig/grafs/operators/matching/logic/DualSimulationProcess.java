package edu.leipzig.grafs.operators.matching.logic;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.CandidateMap;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import edu.leipzig.grafs.util.Sets;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class DualSimulationProcess<W extends Window> extends
    AbstractMatchingProcess<W> {

  public DualSimulationProcess(QueryGraph queryGraph) {
    super(queryGraph);
  }

  static CandidateMap<Vertex> runAlgorithm(Graph graph, QueryGraph queryGraph,
      CandidateMap<Vertex> candidatesMap) {
    EdgeQueryFilter edgeFilter = new EdgeQueryFilter(queryGraph);

    for (var queryVertex : queryGraph.getVertices()) {
      if (!candidatesMap.hasCandidateFor(queryVertex)) {
        return new CandidateMap<>();
      }
    }
    var hasChanged = true;
    while (hasChanged) {
      hasChanged = false;
      for (var querySource : queryGraph.getVertices()) {
        for (var queryTarget : queryGraph.getTargetForSourceVertex(querySource)) {
          var verticesThatHaveParentInCandidates = new HashSet<Vertex>();
          Set<Vertex> deleteCandidates = new HashSet<>();
          for (var sourceCandidate : candidatesMap.getCandidatesFor(querySource)) {
            var targetsOfCandidate = graph.getTargetForSourceVertex(sourceCandidate);
            var candidatesForTargetQuery = candidatesMap.getCandidatesFor(queryTarget);
            var candidatesForTarget = Sets
                .intersection(targetsOfCandidate, candidatesForTargetQuery);
            Predicate<Vertex> hasMatchingEdgeInQuery = (target) -> {
              var edge = graph.getEdgeForVertices(sourceCandidate, target);
              return edgeFilter.filter(edge);
            };
            candidatesForTarget = candidatesForTarget.stream()
                .filter(hasMatchingEdgeInQuery)
                .collect(Collectors.toSet());
            if (candidatesForTarget.isEmpty()) {
              deleteCandidates.add(sourceCandidate);
              hasChanged = true;
            }
            verticesThatHaveParentInCandidates.addAll(candidatesForTarget);
          }
          candidatesMap.removeCandidates(querySource, deleteCandidates);
          if (candidatesMap.getCandidatesFor(querySource).isEmpty()) {
            return new CandidateMap<>();
          }
          if (verticesThatHaveParentInCandidates.isEmpty()) {
            return new CandidateMap<>();
          }
          if (verticesThatHaveParentInCandidates.size() < candidatesMap
              .getCandidatesFor(queryTarget).size()) {
            hasChanged = true;
          }
          candidatesMap.retainCandidates(queryTarget, verticesThatHaveParentInCandidates);
        }
      }
    }
    return candidatesMap;
  }

  @Override
  void processQuery(Graph graph, Collector<EdgeContainer> collector) {
    var dualSimulationMatches = dualSimulationProcess(graph);
    var permutations = makeAllPermutations(dualSimulationMatches);
    var edgeContainerSet = buildEdgeContainerSet(permutations, graph);
    emitEdgeContainer(collector, edgeContainerSet);
  }

  // TODO: Add ability to handle queries with edge properties
  private CandidateMap<Vertex> dualSimulationProcess(Graph graph) {
    var candidatesMap = feasibleVertexMatches(graph);
    return runAlgorithm(graph, queryGraph, candidatesMap);
  }

  private Set<Set<Vertex>> makeAllPermutations(CandidateMap<Vertex> candidateMap) {
    return buildPermutations(candidateMap.asListOfCandidateSets());
  }

}
