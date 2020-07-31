package edu.leipzig.grafs.model;


import edu.leipzig.grafs.util.MultiMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.gradoop.common.model.impl.id.GradoopId;

public class Graph {

  protected final Set<Vertex> vertices;
  protected final Map<GradoopId, Vertex> vertexMap;
  protected final Set<Edge> edges;
  protected final MultiMap<GradoopId, Edge> sourceToEdgeMap;
  protected final MultiMap<GradoopId, Edge> targetToEdgeMap;

  public Graph() {
    this(new HashSet<>(), new HashSet<>());
  }

  public Graph(Collection<Vertex> vertices, Collection<Edge> edges) {
    this.vertices = new HashSet<>(vertices);
    this.edges = new HashSet<>(edges);
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
      addedNewVertex = addedNewVertex || addVertex(vertex);
    }

    return addedNewVertex;
  }


  public Set<Vertex> getVertices() {
    return vertices;
  }

  public boolean addEdge(Edge edge) {
    if (vertexMap.containsKey(edge.getSourceId())) {
      throw new RuntimeException(String.format("No source vertex found for edge %s", edge));
    }
    if (vertexMap.containsKey(edge.getTargetId())) {
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
      addedNewEdge = addedNewEdge || addEdge(edge);
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
    return sourceToEdgeMap.get(vertex.getId());
  }

  public Set<Edge> getEdgesForTarget(Vertex vertex) {
    return targetToEdgeMap.get(vertex.getId());
  }

  public Set<Vertex> getAdjacentFor(Vertex vertex) {
    var edgesOfSource = sourceToEdgeMap.get(vertex.getId());
    return edgesOfSource.stream()
        .map(Edge::getTargetId)
        .map(vertexMap::get)
        .collect(Collectors.toSet());
  }

  private void createMaps(Collection<Vertex> vertices, Collection<Edge> edges) {
    addVertices(vertices);
    addEdges(edges);
  }

}
