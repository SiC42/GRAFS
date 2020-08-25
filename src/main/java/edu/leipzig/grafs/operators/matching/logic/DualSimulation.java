package edu.leipzig.grafs.operators.matching.logic;

import static edu.leipzig.grafs.operators.matching.logic.ElementMatcher.matchesQueryElem;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.GraphElement;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import edu.leipzig.grafs.util.MultiMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class DualSimulation<W extends Window> extends
    ProcessAllWindowFunction<EdgeContainer, EdgeContainer, W> {

  private final QueryGraph queryGraph;

  public DualSimulation(QueryGraph pattern) {
    queryGraph = pattern;
  }

  @Override
  public void process(Context context, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> collector) {
    var graph = Graph.fromEdgeContainers(ecIterable);
    if (queryGraph.isVertexOnly()) {
      processVerticesOnly(graph, collector);
    } else {
      processEdges(graph, collector);
    }

  }

  private void processVerticesOnly(Graph graph,
      Collector<EdgeContainer> collector) {

  }

  void processEdges(Graph graph, Collector<EdgeContainer> collector) {

    var graphs = processEdges(graph);

  }

  // TODO: Add ability to handle queries with edge properties
  Set<Graph> processEdges(Graph graph) {

    var candidatesMap = buildCandidates(graph);
    var dualSimCandidates = createDualSimulationCandidates(graph, queryGraph, candidatesMap);
    return search(graph, queryGraph, dualSimCandidates);

  }

  Candidates<Vertex> buildCandidates(Graph graph) {
    Candidates<Vertex> sim = new Candidates<>();
    for (var queryVertex : queryGraph.getVertices()) {
      for (var vertex : graph.getVertices()) {
        if (matchesQueryElem(queryVertex, vertex)) {
          sim.addCandidate(queryVertex, vertex);
        }
      }
    }
    return sim;
  }

  Candidates<Vertex> createDualSimulationCandidates(Graph graph, QueryGraph queryGraph,
      Candidates<Vertex> candidatesMap) {
    for (var queryVertex : queryGraph.getVertices()) {
      if (!candidatesMap.hasCandidateFor(queryVertex)) {
        return new Candidates<>();
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
            var candidatesForTarget = intersection(targetsOfCandidate, candidatesForTargetQuery);
            if (candidatesForTarget.isEmpty()) {
              deleteCandidates.add(sourceCandidate);
              hasChanged = true;
            }
            verticesThatHaveParentInCandidates.addAll(candidatesForTarget);
          }
          candidatesMap.deleteCandidates(querySource, deleteCandidates);
          if (candidatesMap.getCandidatesFor(querySource).isEmpty()) {
            return new Candidates<>();
          }
          if (verticesThatHaveParentInCandidates.isEmpty()) {
            return new Candidates<>();
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

  private <E> Set<E> intersection(Set<E> set1, Set<E> set2) {
    var intersection = new HashSet<>(set1);
    intersection.retainAll(set2);
    return intersection;
  }


  Set<Graph> search(Graph graph, QueryGraph queryGraph,
      Candidates<Vertex> candidates) {
    var qVertexArray = queryGraph.getVertices().toArray(new Vertex[0]);
    return search(graph, qVertexArray, candidates, 0);
  }

  Set<Graph> search(Graph graph, Vertex[] qVertexArray,
      Candidates<Vertex> candidateMap, int depth) {
    Set<Graph> matches = new HashSet<>();
    if (depth == qVertexArray.length) {
      Set<Vertex> matchedVertices = new HashSet<>();
      for (int i = 0; i < qVertexArray.length - 1; i++) {
        matchedVertices.add(candidateMap.getCandidatesFor(qVertexArray[i]).iterator().next());
      }
      var lastQueryVertex = qVertexArray[qVertexArray.length - 1];
      for (var candidate : candidateMap.getCandidatesFor(lastQueryVertex)) {
        Set<Vertex> finalMatchedVertices = new HashSet<>(matchedVertices);
        finalMatchedVertices.add(candidate);
        matches.add(graph.getVertexInducedSubGraph(finalMatchedVertices));
      }
    } else {
      var currentQueryVertex = qVertexArray[depth];
      for (var candidate : candidateMap.getCandidatesFor(qVertexArray[depth])) {
        if (notAlreadyCandidate(candidate, candidateMap, qVertexArray, depth)) {
          var copyOfCandidateMap = new Candidates<Vertex>();
          copyOfCandidateMap.addCandidates(candidateMap);
          // set candidate for current query vertex  to the selected candidate
          copyOfCandidateMap.removeAll(currentQueryVertex);
          copyOfCandidateMap.addCandidate(currentQueryVertex, candidate);

          copyOfCandidateMap = createDualSimulationCandidates(graph, queryGraph,
              copyOfCandidateMap);
          if (!copyOfCandidateMap.isEmpty()) {
            var recursiveMatches = search(graph, qVertexArray, copyOfCandidateMap, depth + 1);
            matches.addAll(recursiveMatches);
          }
        }
      }
    }
    return matches;
  }

  private boolean notAlreadyCandidate(Vertex candidate, Candidates<Vertex> candidateMap,
      Vertex[] qVertexArray, int depth) {
    for (int i = 0; i < depth; i++) {
      var candidates = candidateMap.getCandidatesFor(qVertexArray[i]);
      if (candidates.contains(candidate)) {
        return false;
      }
    }
    return true;
  }


  static class Candidates<E extends GraphElement> {

    private final MultiMap<E, E> candidateMap;

    public Candidates() {
      candidateMap = new MultiMap<>();
    }

    public boolean addCandidate(E keyElem, E elem) {
      return candidateMap.put(keyElem, elem);
    }

    public boolean hasCandidateFor(E keyElem) {
      return candidateMap.containsKey(keyElem) &&
          !candidateMap.get(keyElem).isEmpty();
    }

    public Set<E> getCandidatesFor(E keyElem) {
      return candidateMap.get(keyElem);
    }

    public boolean hasCandidate(E keyElem, E elem) {
      return candidateMap.get(keyElem).contains(elem);
    }

    public void deleteCandidates(E keyElem, Collection<E> deleteCollection) {
      candidateMap.removeAll(keyElem, deleteCollection);
    }

    public void deleteCandidate(E keyElem, E elemToDelete) {
      candidateMap.remove(keyElem, elemToDelete);
    }

    public void addCandidates(E keyElem, Set<E> newRelationships) {
      candidateMap.putAll(keyElem, newRelationships);
    }

    public void addCandidates(Candidates<E> candidates) {
      for (var keyElem : candidates.candidateMap.keySet()) {
        addCandidates(keyElem, candidates.getCandidatesFor(keyElem));
      }
    }

    public MultiMap<E, E> asMultiMap() {
      return candidateMap;
    }

    public boolean isEmpty() {
      return candidateMap.isEmpty();
    }

    public int size() {
      return candidateMap.size();
    }

    @Override
    public String toString() {
      return "Candidates{" + candidateMap + '}';
    }

    public boolean retainCandidates(E keyElem, Collection<E> retainCollection) {
      return candidateMap.retainAll(keyElem, retainCollection);
    }

    public void removeAll(E key) {
      candidateMap.removeAll(key);
    }

    public Set<E> keySet() {
      return candidateMap.keySet();
    }
  }
}
