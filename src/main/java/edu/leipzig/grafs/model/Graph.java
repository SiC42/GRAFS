package edu.leipzig.grafs.model;


import edu.leipzig.grafs.util.MultiMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.gradoop.common.model.impl.id.GradoopId;

public class Graph {

  protected final Set<Vertex> vertices;
  protected final Map<GradoopId, Vertex> vertexMap;
  protected final Set<Edge> edges;
  protected final MultiMap<GradoopId, Edge> sourceToEdgeMap;
  protected final MultiMap<GradoopId, Edge> targetToEdgeMap;
  private final GradoopId id;

  public Graph() {
    this(new HashSet<>(), new HashSet<>());
  }

  public Graph(Collection<Vertex> vertices, Collection<Edge> edges) {
    this(GradoopId.get(), vertices, edges);
  }

  public Graph(GradoopId graphId, Collection<Vertex> vertices, Collection<Edge> edges) {
    this.id = graphId;
    this.vertices = new HashSet<>();
    this.edges = new HashSet<>();
    vertexMap = new HashMap<>();
    sourceToEdgeMap = new MultiMap<>();
    targetToEdgeMap = new MultiMap<>();
    createMaps(vertices, edges);
  }

  public static Graph fromEdgeContainers(Iterable<EdgeContainer> ecIterable) {
    var graph = new Graph();
    for (var ec : ecIterable) {
      graph.addVertex(ec.getSourceVertex());
      graph.addVertex(ec.getTargetVertex());
      graph.addEdge(ec.getEdge());
    }
    return graph;
  }

  public GradoopId getId() {
    return id;
  }

  public boolean addVertex(Vertex vertex) {
    boolean isNewVertex = vertices.add(vertex);
    if (isNewVertex) {
      vertexMap.put(vertex.getId(), vertex);
    }
    return isNewVertex;
  }

  public boolean addVertices(Collection<Vertex> vertices) {
    boolean addedNewVertex = false;
    for (var vertex : vertices) {
      if (addVertex(vertex)) {
        addedNewVertex = true;
      }
    }

    return addedNewVertex;
  }


  public Set<Vertex> getVertices() {
    return vertices;
  }

  public boolean addEdge(Edge edge) {
    if (!vertexMap.containsKey(edge.getSourceId())) {
      System.out.println(vertices);
      throw new RuntimeException(
          String.format("No source vertex with id %s found for edge %s", edge.getSourceId(), edge));
    }
    if (!vertexMap.containsKey(edge.getTargetId())) {
      throw new RuntimeException(String.format("No target vertex found for edge %s", edge));
    }
    boolean isNewEdge = edges.add(edge);
    if (isNewEdge) {
      sourceToEdgeMap.put(edge.getSourceId(), edge);
      targetToEdgeMap.put(edge.getTargetId(), edge);
    }
    return isNewEdge;
  }

  public boolean addEdges(Collection<Edge> edges) {
    var addedNewEdge = false;
    for (var edge : edges) {
      if (addEdge(edge)) {
        addedNewEdge = true;
      }
    }
    return addedNewEdge;
  }

  public Set<Edge> getEdges() {
    return edges;
  }

  public Vertex getSourceForEdge(Edge edge) {
    return vertexMap.get(edge.getSourceId());
  }

  public Vertex getTargetForEdge(Edge edge) {
    return vertexMap.get(edge.getTargetId());
  }

  public Set<Edge> getEdgesForSource(Vertex vertex) {
    if (sourceToEdgeMap.containsKey(vertex.getId())) {
      return sourceToEdgeMap.get(vertex.getId());
    }
    return Collections.emptySet();
  }

  public Set<Edge> getEdgesForTarget(Vertex vertex) {
    if (targetToEdgeMap.containsKey(vertex.getId())) {
      return targetToEdgeMap.get(vertex.getId());
    }
    return Collections.emptySet();
  }

  public Set<Vertex> getTargetForSourceVertex(Vertex vertex) {
    var edgesOfSource = sourceToEdgeMap.get(vertex.getId());
    if (edgesOfSource == null) {
      return new HashSet<>();
    }
    return edgesOfSource.stream()
        .map(Edge::getTargetId)
        .map(vertexMap::get)
        .collect(Collectors.toSet());
  }

  // TODO: Find a cheaper way for this
  public Edge getEdgeForVertices(Vertex sourceVertex, Vertex targetVertex) {
    Set<Edge> edgeIntersection = new HashSet<>(sourceToEdgeMap.get(sourceVertex.getId()));
    edgeIntersection.retainAll(targetToEdgeMap.get(targetVertex.getId()));
    Iterator<Edge> iterator = edgeIntersection.iterator();
    return iterator.hasNext() ? iterator.next() : null;
  }

  public Graph getVertexInducedSubGraph(Collection<Vertex> vertices) {
    Graph vInducedSubGraph = new Graph(vertices, new HashSet<>());
    for (var vertex : vertices) {
      for (var edge : getEdgesForSource(vertex)) {
        var target = getTargetForEdge(edge);
        if (vertices.contains(target)) {
          vInducedSubGraph.addEdge(edge);
        }
      }
    }
    return vInducedSubGraph;
  }

  private void createMaps(Collection<Vertex> vertices, Collection<Edge> edges) {
    addVertices(vertices);
    addEdges(edges);
  }

  @Override
  public String toString() {
    return "Graph{" +
        "vertices=" + vertices +
        ", edges=" + edges +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Graph graph = (Graph) o;
    return Objects.equals(vertices, graph.vertices) &&
        Objects.equals(vertexMap, graph.vertexMap) &&
        Objects.equals(edges, graph.edges) &&
        Objects.equals(sourceToEdgeMap, graph.sourceToEdgeMap) &&
        Objects.equals(targetToEdgeMap, graph.targetToEdgeMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(vertices, vertexMap, edges, sourceToEdgeMap, targetToEdgeMap);
  }
}
