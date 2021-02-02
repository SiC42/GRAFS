package edu.leipzig.grafs.operators.matching.logic;

import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.CandidateMap;
import edu.leipzig.grafs.util.Sets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Applies a dual simulation algorithm (based on <a href="https://ieeexplore.ieee.org/abstract/document/6906821">"DualIso:
 * An Algorithm for Subgraph Pattern Matching on Very Large Labeled Graphs"</a> by Saltz et al.)
 *
 * @param <W> type of window used
 */
public class DualSimulationProcess<W extends Window> extends
    AbstractMatchingProcess<W> {

  /**
   * Initialized proces with given query graph.
   *
   * @param queryGraph query graph which should be used for the dual simulation process.
   */
  public DualSimulationProcess(Graph queryGraph) {
    super(queryGraph);
  }

  /**
   * Algorithm based on the mentioned paper paper. Creates a map of dual simulation matches.
   *
   * @param graph         graph for which the matching patterns should be found
   * @param queryGraph    query graph which is used as the pattern
   * @param candidatesMap map of viable candidates (i.e. pruned subset of all vertices in the
   *                      graph)
   * @return a map of dual simulation matches
   * @see <a href="https://ieeexplore.ieee.org/abstract/document/6906821">"DualIso: An Algorithm for
   * Subgraph Pattern Matching on Very Large Labeled Graphs"</a> by Saltz et al.
   */
  static CandidateMap<Vertex> runAlgorithm(Graph graph, Graph queryGraph,
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
      //5: for u ← Vq do
      for (var querySource : queryGraph.getVertices()) {
        //6: for u' ← Q.adj(u) do
        for (var queryTarget : queryGraph.getTargetForSourceVertex(querySource)) {
          //7: Φ'(u') ← ∅
          var verticesThatHaveParentInCandidates = new HashSet<Vertex>();
          Set<Vertex> deleteCandidates = new HashSet<>();
          //8: for v ← Φ(u) do
          for (var sourceCandidate : candidatesMap.getCandidatesFor(querySource)) {
            var targetsOfCandidate = graph.getTargetForSourceVertex(sourceCandidate);
            var candidatesForTargetQuery = candidatesMap.getCandidatesFor(queryTarget);
            //9: Φ_v(u') ← G.adj(v) ∩ Φ(u')
            var candidatesForTarget = Sets
                .intersection(targetsOfCandidate, candidatesForTargetQuery);
            Predicate<Vertex> hasMatchingEdgeInQuery = (target) -> {
              var edge = graph.getEdgeForVertices(sourceCandidate, target);
              return edgeFilter.filter(edge);
            };
            //10+11: if Φ_v(u') = ∅ then remove v from Φ(u)
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
          //12: if Φ(u) = ∅ then
          if (candidatesMap.getCandidatesFor(querySource).isEmpty()) {
            return new CandidateMap<>();
          }
          //16: if Φ'(u') = ∅ then
          if (verticesThatHaveParentInCandidates.isEmpty()) {
            return new CandidateMap<>();
          }
          //18: if Φ(u) is smaller than Φ(u) then
          if (verticesThatHaveParentInCandidates.size() < candidatesMap
              .getCandidatesFor(queryTarget).size()) {
            hasChanged = true;
          }
          //20: Φ(u) = Φ(u) ∩ Φ(u)
          candidatesMap.retainCandidates(queryTarget, verticesThatHaveParentInCandidates);
        }
      }
    }
    return candidatesMap;
  }

  /**
   * Processes the given window graph and applies the algorithm.
   *
   * @param graph     Graph for which the process should find matches
   * @param collector outputs the matched elements
   */
  @Override
  void processQuery(Graph graph, Collector<Triplet> collector) {
    var dualSimulationMatches = dualSimulationProcess(graph);
    if (dualSimulationMatches.isEmpty()) {
      return;
    }
    var getAllVertices = dualSimulationMatches
        .asListOfCandidateSets()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
    var subgraph = graph.getVertexInducedSubGraph(getAllVertices);
    var tripletFactory = new TripletFactory();
    for (var edge : subgraph.getEdges()) {
      if (matchesAnyQueryEdge(edge, graph)) {
        var source = graph.getSourceForEdge(edge);
        var target = graph.getTargetForEdge(edge);
        tripletFactory.add(edge, source, target, subgraph.getId());
      }
    }
    emitTriplet(collector, tripletFactory.getTriplets());
  }

  private CandidateMap<Vertex> dualSimulationProcess(Graph graph) {
    var candidatesMap = feasibleVertexMatches(graph);
    return runAlgorithm(graph, queryGraph, candidatesMap);
  }

}
