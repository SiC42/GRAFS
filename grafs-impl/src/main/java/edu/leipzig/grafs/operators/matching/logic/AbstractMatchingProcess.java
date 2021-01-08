package edu.leipzig.grafs.operators.matching.logic;

import static edu.leipzig.grafs.operators.matching.logic.ElementMatcher.matchesQueryElem;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Graph;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.matching.model.CandidateMap;
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
    ProcessAllWindowFunction<Triplet, Triplet, W> {

  final Graph queryGraph;

  /**
   * Initializes the matching process with the given query graph
   *
   * @param queryGraph graph for which the operator should find patterns
   */
  AbstractMatchingProcess(Graph queryGraph) {
    this.queryGraph = queryGraph;
  }

  /**
   * Applies the pattern matching process onto the elements in the window.
   *
   * @param notUsed    not used
   * @param tripletIt all triplet in the window
   * @param collector  outputs the matched elements
   */
  @Override
  public void process(Context notUsed, Iterable<Triplet> tripletIt,
      Collector<Triplet> collector) {
    var graph = Graph.fromTriplets(tripletIt);
    if (queryGraph.getEdges().isEmpty()) {
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
  abstract void processQuery(Graph graph, Collector<Triplet> collector);

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
   * resulting triplet.
   * <p>
   * In this process an {@link TripletFactory} is used to ensure that the triplets are
   * only emitted ones and the elements in the triplet have new graph ids applied to represent
   * the new graph.
   *
   * @param setOfMatches a set of sets of vertices (the inner set represents one match)
   * @param graph        original graph used to get the vertex induced subgraphs
   * @return collection of triplets which match the pattern
   */
  Collection<Triplet> buildTripletSet(Set<Set<Vertex>> setOfMatches, Graph graph) {
    var tripletFactory = new TripletFactory();
    for (var vertexSet : setOfMatches) {
      var inducedGraph = graph.getVertexInducedSubGraph(vertexSet);
      for (var edge : inducedGraph.getEdges()) {
        if (matchesAnyQueryEdge(edge, graph)) {
          var source = graph.getSourceForEdge(edge);
          var target = graph.getTargetForEdge(edge);
          tripletFactory.add(edge, source, target, inducedGraph.getId());
        }
      }
    }
    return tripletFactory.getTriplets();
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
   * Emits the given triplets via the collector.
   *
   * @param collector      used to emit the triplet
   * @param triplets triplets which should be emitted
   */
  void emitTriplet(Collector<Triplet> collector,
      Collection<Triplet> triplets) {
    for (var triplet : triplets) {
      collector.collect(triplet);
    }
  }

  /**
   * Returns <tt>true</tt> if the given vertex candidate was not used before, i.e. is not a
   * candidate for any query vertex in the array with an index smaller than the given depth.
   *
   * @param candidate element to be tested if it was not a candidate before
   * @param candidateMap candidate map to be looked at for element
   * @param qVertexArray query elements of previous iterations used as key for map
   * @param depth current depth, used to find out which query elements where used
   * @return <tt>true</tt> if the given vertex candidate was not used before
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
  static class TripletFactory {

    private final Map<GradoopId, Edge> edgeMap;
    private final Map<GradoopId, Vertex> vertexMap;

    public TripletFactory() {
      edgeMap = new HashMap<>();
      vertexMap = new HashMap<>();
    }

    public boolean contains(Triplet triplet) {
      return edgeMap.containsKey(triplet.getEdge().getId());
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

    public Collection<Triplet> getTriplets() {
      Function<Edge, Triplet> createTriplet = e ->
          new Triplet(e, vertexMap.get(e.getSourceId()), vertexMap.get(e.getTargetId()));
      return edgeMap.values().stream()
          .map(createTriplet)
          .collect(Collectors.toCollection(HashSet::new));
    }

  }

}
