package edu.leipzig.grafs.operators.matching.logic;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.CandidateMap;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class IsomorphismMatchingProcess<W extends Window> extends AbstractMatchingProcess<W> {


  public IsomorphismMatchingProcess(QueryGraph queryGraph) {
    super(queryGraph);
  }

  @Override
  void processQuery(Graph graph, Collector<EdgeContainer> collector) {
    var candidatesMap = feasibleVertexMatches(graph);
    if(candidatesMap.isEmpty()) {
      return;
    }
      var dualSimCandidates = DualSimulationProcess
          .runAlgorithm(graph, queryGraph, candidatesMap);
    if(dualSimCandidates.isEmpty()) {
      return;
    }
    var permutations = search(graph, queryGraph, dualSimCandidates);
    var edgeContainerSet = buildEdgeContainerSet(permutations, graph);
    emitEdgeContainer(collector, edgeContainerSet);
  }


  private Set<Set<Vertex>> search(Graph graph, QueryGraph queryGraph,
      CandidateMap<Vertex> candidateMap) {
    var qVertexArray = queryGraph.getVertices().toArray(new Vertex[0]);
    return search(graph, qVertexArray, candidateMap, 0);
  }

  private Set<Set<Vertex>> search(Graph graph, Vertex[] qVertexArray,
      CandidateMap<Vertex> candidateMap, int depth) {
    Set<Set<Vertex>> matches = new HashSet<>();
    if (depth == qVertexArray.length) {
      matches = buildPermutations(candidateMap.asListOfCandidateSets());
    } else {
      var currentQueryVertex = qVertexArray[depth];
      for (var candidate : candidateMap.getCandidatesFor(qVertexArray[depth])) {
        if (previouslyNotCandidate(candidate, candidateMap, qVertexArray, depth)) {
          var prunedCandidateMap = new CandidateMap<Vertex>();
          prunedCandidateMap.addCandidates(candidateMap);
          // set candidate for current query vertex  to the selected candidate
          prunedCandidateMap.removeCandidates(currentQueryVertex);
          prunedCandidateMap.addCandidate(currentQueryVertex, candidate);
          prunedCandidateMap = DualSimulationProcess
              .runAlgorithm(graph, queryGraph, prunedCandidateMap);
          if (!prunedCandidateMap.isEmpty()) {
            var recursiveMatches = search(graph, qVertexArray, prunedCandidateMap, depth + 1);
            matches.addAll(recursiveMatches);
          }
        }
      }
    }
    return matches;
  }

}
