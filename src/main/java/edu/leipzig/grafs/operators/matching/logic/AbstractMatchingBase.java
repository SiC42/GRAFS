package edu.leipzig.grafs.operators.matching.logic;

import static edu.leipzig.grafs.operators.matching.logic.ElementMatcher.matchesQueryElem;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.CandidateMap;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;

public abstract class AbstractMatchingBase<W extends Window> extends
    ProcessAllWindowFunction<EdgeContainer, EdgeContainer, W> {

  final QueryGraph queryGraph;

  AbstractMatchingBase(QueryGraph queryGraph) {
    this.queryGraph = queryGraph;
  }

  @Override
  public void process(Context context, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> collector) {
    var graph = Graph.fromEdgeContainers(ecIterable);
    if (queryGraph.isVertexOnly()) {
      throw new RuntimeException(
          "Can't process query with only vertices, because only edge stream model is supported");
    } else {
      processQuery(graph, collector);
    }

  }

  abstract void processQuery(Graph graph, Collector<EdgeContainer> collector);


  CandidateMap<Vertex> feasibleVertexMatches(Graph graph) {
    CandidateMap<Vertex> sim = new CandidateMap<>();
    for (var queryVertex : queryGraph.getVertices()) {
      for (var vertex : graph.getVertices()) {
        if (matchesQueryElem(queryVertex, vertex)) {
          sim.addCandidate(queryVertex, vertex);
        }
      }
    }
    return sim;
  }

  Collection<EdgeContainer> buildEdgeContainerSet(Set<Set<Vertex>> vertexSets, Graph graph) {
    var updatableEcSet = new EdgeContainerFactory();
    for (var vertexSet : vertexSets) {
      var inducedGraph = graph.getVertexInducedSubGraph(vertexSet);
      for (var edge : inducedGraph.getEdges()) {
        var source = graph.getSourceForEdge(edge);
        var target = graph.getTargetForEdge(edge);
        updatableEcSet.add(edge, source, target, inducedGraph.getId());
      }
    }
    return updatableEcSet.getEdgeContainers();
  }

  void emitEdgeContainer(Collector<EdgeContainer> collector,
      Collection<EdgeContainer> updatableEcCol) {
    for (var ec : updatableEcCol) {
      collector.collect(ec);
    }
  }

  boolean notAlreadyCandidate(Vertex candidate, CandidateMap<Vertex> candidateMap,
      Vertex[] qVertexArray, int depth) {
    for (int i = 0; i < depth; i++) {
      var candidates = candidateMap.getCandidatesFor(qVertexArray[i]);
      if (candidates.contains(candidate)) {
        return false;
      }
    }
    return true;
  }

  Set<Set<Vertex>> buildPermutations(ArrayList<Set<Vertex>> listOfCandidateSets) {
    return buildPermutations(listOfCandidateSets, 0);
  }

  private Set<Set<Vertex>> buildPermutations(ArrayList<Set<Vertex>> listOfCandidateSets,
      int depth) {
    Set<Set<Vertex>> permutations = new HashSet<>();
    if (depth == listOfCandidateSets.size() - 1) {
      for (var candidate : listOfCandidateSets.get(depth)) {
        permutations.add(Set.of(candidate));
      }
    } else {
      for (var candidate : listOfCandidateSets.get(depth)) {
        var deeperPermutations = buildPermutations(listOfCandidateSets, depth + 1);
        for (var permutation : deeperPermutations) {
          var updatedPermutation = new HashSet<>(permutation);
          updatedPermutation.add(candidate);
          permutations.add(updatedPermutation);
        }
      }
    }
    return permutations;
  }

  /**
   * Builds the new graph
   */
  static class EdgeContainerFactory {

    private final Map<GradoopId, Edge> edgeMap;
    private final Map<GradoopId, Vertex> vertexMap;

    public EdgeContainerFactory() {
      edgeMap = new HashMap<>();
      vertexMap = new HashMap<>();
    }

    public boolean contains(EdgeContainer ec) {
      return edgeMap.containsKey(ec.getEdge().getId());
    }

    public void add(Edge edge, Vertex source, Vertex target, GradoopId graphId) {
      if (edgeMap.containsKey(edge.getId())) {
        edge = edgeMap.get(edge.getId());
        source = vertexMap.get(edge.getSourceId());
        target = vertexMap.get(edge.getTargetId());
      }
      edge.addGraphId(graphId);
      source.addGraphId(graphId);
      target.addGraphId(graphId);
      edgeMap.put(edge.getId(), edge);
      vertexMap.put(source.getId(), source);
      vertexMap.put(target.getId(), target);
    }

    public Collection<EdgeContainer> getEdgeContainers() {
      Function<Edge, EdgeContainer> createEdgeFactory = e ->
          new EdgeContainer(e, vertexMap.get(e.getSourceId()), vertexMap.get(e.getTargetId()));
      return edgeMap.values().stream()
          .map(createEdgeFactory)
          .collect(Collectors.toCollection(HashSet::new));
    }

  }

}
