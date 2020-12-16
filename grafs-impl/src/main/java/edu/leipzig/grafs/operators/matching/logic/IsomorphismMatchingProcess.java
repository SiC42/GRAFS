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

/**
 * Applies a isomorphism matching algorithm (based on <a href="https://ieeexplore.ieee.org/abstract/document/6906821">"DualIso:
 * An Algorithm for Subgraph Pattern Matching on Very Large Labeled Graphs"</a> by Saltz et al.)
 *
 * @param <W> type of window used
 */
public class IsomorphismMatchingProcess<W extends Window> extends AbstractMatchingProcess<W> {

  /**
   * Initialized proces with given query graph.
   *
   * @param queryGraph query graph which should be used for the dual simulation process.
   */
  public IsomorphismMatchingProcess(QueryGraph queryGraph) {
    super(queryGraph);
  }

  /**
   * Processes the given window graph and applies the algorithm.
   *
   * @param graph     Graph for which the process should find matches
   * @param collector outputs the matched elements
   */
  @Override
  void processQuery(Graph graph, Collector<EdgeContainer> collector) {
    var candidatesMap = feasibleVertexMatches(graph);
    if (candidatesMap.isEmpty()) {
      return;
    }
    var dualSimCandidates = DualSimulationProcess
        .runAlgorithm(graph, queryGraph, candidatesMap);
    if (dualSimCandidates.isEmpty()) {
      return;
    }
    var permutations = search(graph, dualSimCandidates);
    var edgeContainerSet = buildEdgeContainerSet(permutations, graph);
    emitEdgeContainer(collector, edgeContainerSet);
  }

  /**
   * Algorithm based on the mentioned paper paper. Creates set of all isomorphism matches.
   *
   * @param graph        Graph for which the process should find matches
   * @param candidateMap map of viable candidates (i.e. pruned subset of all vertices in the graph)
   * @return set of all matches
   * @see <a href="https://ieeexplore.ieee.org/abstract/document/6906821">"DualIso: An Algorithm for
   * Subgraph Pattern Matching on Very Large Labeled Graphs"</a> by Saltz et al.
   */
  private Set<Set<Vertex>> search(Graph graph,
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
