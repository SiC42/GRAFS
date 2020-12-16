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

/**
 * Abstract class which is used to provide basic methods for the specific matching processes.
 *
 * @param <W> type of window used
 */
public abstract class AbstractMatchingProcess<W extends Window> extends
    ProcessAllWindowFunction<EdgeContainer, EdgeContainer, W> {

  final QueryGraph queryGraph;

  /**
   * Initializes the matching process with the given query graph
   *
   * @param queryGraph graph for which the operator should find patterns
   */
  AbstractMatchingProcess(QueryGraph queryGraph) {
    this.queryGraph = queryGraph;
  }

  /**
   * Applies the pattern matching process onto the elements in the window.
   *
   * @param notUsed    not used
   * @param ecIterable all edge container in the window
   * @param collector  outputs the matched elements
   */
  @Override
  public void process(Context notUsed, Iterable<EdgeContainer> ecIterable,
      Collector<EdgeContainer> collector) {
    var graph = Graph.fromEdgeContainers(ecIterable);
    if (queryGraph.isVertexOnly()) {
      throw new RuntimeException(
          "Can't process query with only vertices, because only edge stream model is supported");
    } else {
      processQuery(graph, collector);
    }

  }

  /**
   * Applies the specific pattern matching process onto the elements in the window.
   *
   * @param graph     Graph for which the process should find matches
   * @param collector outputs the matched elements
   */
  abstract void processQuery(Graph graph, Collector<EdgeContainer> collector);

  /**
   * Returns all feasible vertex matches in the window, i.e. all vertices for which there is a
   * matching vertex in the query graph (ignoring edges).
   *
   * @param graph graph from which the vertices should be used to match the query graph
   * @return a map of all candidates for every query vertex
   */
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

  /**
   * Given a set of sets of vertices (the inner set represents one match), the method constructs the
   * resulting edge container.
   * <p>
   * In this process an {@link EdgeContainerFactory} is used to ensure that the edge container are
   * only emitted ones and the elements in the containers have new graph ids applied to represent
   * the new graph.
   *
   * @param setOfMatches a set of sets of vertices (the inner set represents one match)
   * @param graph        original graph used to get the vertex induced subgraphs
   * @return collection of edge containers which match the pattern
   */
  Collection<EdgeContainer> buildEdgeContainerSet(Set<Set<Vertex>> setOfMatches, Graph graph) {
    var ecFactory = new EdgeContainerFactory();
    for (var vertexSet : setOfMatches) {
      var inducedGraph = graph.getVertexInducedSubGraph(vertexSet);
      for (var edge : inducedGraph.getEdges()) {
        if (matchesAnyQueryEdge(edge, graph)) {
          var source = graph.getSourceForEdge(edge);
          var target = graph.getTargetForEdge(edge);
          ecFactory.add(edge, source, target, inducedGraph.getId());
        }
      }
    }
    return ecFactory.getEdgeContainers();
  }

  /**
   * Returns <tt>true</tt> if the given edge matches and edge in the query graph.
   *
   * @param edge  edge for which a match should be search
   * @param graph graph used to get the source and target of the given edge
   * @return <tt>true</tt> if the given edge matches and edge in the query graph
   */
  private boolean matchesAnyQueryEdge(Edge edge, Graph graph) {
    EdgeQueryFilter edgeFilter = new EdgeQueryFilter(queryGraph);
    var source = graph.getSourceForEdge(edge);
    var target = graph.getTargetForEdge(edge);
    return edgeFilter.filter(edge, source, target);
  }

  /**
   * Emits the given edge containers via the collector.
   *
   * @param collector      used to emit the edge container
   * @param edgeContainers edge containers which should be emitted
   */
  void emitEdgeContainer(Collector<EdgeContainer> collector,
      Collection<EdgeContainer> edgeContainers) {
    for (var ec : edgeContainers) {
      collector.collect(ec);
    }
  }

  /**
   * Returns <tt>true</tt> if the given vertex candidate was not used before, i.e. is not a
   * candidate for any query vertex in the array with an index smaller than the given depth.
   *
   * @param candidate
   * @param candidateMap
   * @param qVertexArray
   * @param depth
   * @return
   */
  boolean previouslyNotCandidate(Vertex candidate, CandidateMap<Vertex> candidateMap,
      Vertex[] qVertexArray, int depth) {
    for (int i = 0; i < depth; i++) {
      var candidates = candidateMap.getCandidatesFor(qVertexArray[i]);
      if (candidates.contains(candidate)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Builds all possible permutations of the candidate set as vertex sets. E.g. if the candidate set
   * is ({1,2}, {1, 3, 4}), the resulting permutation sets are: {{1},{1,3},{1,4},{2,1},{2,3},{2,4}}
   * <p>
   * With this the graphs can easily be build
   *
   * @param listOfCandidateSets a list of candidates for each query vertex
   * @return all matches
   */
  Set<Set<Vertex>> buildPermutations(ArrayList<Set<Vertex>> listOfCandidateSets) {
    return buildPermutations(listOfCandidateSets, 0);
  }

  /**
   * Builds all possible permutations of the candidate set as vertex sets based on depth.
   *
   * @param listOfCandidateSets a list of candidates for each query vertex
   * @param depth               all matches
   * @return all matches for the given depth
   * @see #buildPermutations(ArrayList)
   */
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
   * Builds the new graph.
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
