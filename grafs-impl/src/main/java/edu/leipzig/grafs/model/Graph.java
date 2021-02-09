package edu.leipzig.grafs.model;


import edu.leipzig.grafs.exceptions.VertexNotPartOfTheGraphException;
import edu.leipzig.grafs.util.MultiMap;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Data model that represents the graph in the property graph model (with graph membership).
 */
public class Graph extends BasicGraph<Vertex, Edge> {

  /**
   * Constructs an empty graph.
   */
  public Graph() {
    super(new HashSet<>(), new HashSet<>());
  }

  /**
   * Constructs a new graph (with a new ID) with the given vertices and edges.
   *
   * @param vertices vertices that should be part of the graph
   * @param edges    edges that should be part of the graph
   */
  public Graph(Collection<Vertex> vertices, Collection<Edge> edges) {
    super(vertices,edges);
  }

  /**
   * Constructs a new graph properties.
   *
   * @param vertices vertices that should be part of the graph
   * @param edges    edges that should be part of the graph
   * @param graphId  ID to be used for the graph
   */
  public Graph(GradoopId graphId, Collection<Vertex> vertices, Collection<Edge> edges) {
    super(graphId, new HashSet<>(), new HashSet<>(), vertices, edges);
  }

  /**
   * Constructs a new graph properties.
   *
   * @param vertices vertices that should be part of the graph
   * @param edges    edges that should be part of the graph
   * @param graphId  ID to be used for the graph
   */
  protected Graph(GradoopId graphId, Collection<Vertex> newVColl, Collection<Edge> newEColl, Collection<Vertex> vertices, Collection<Edge> edges) {
    super(graphId, newVColl, newEColl, vertices, edges);
  }

  /**
   * Constructs a graph from an triplet iterable and returns it.
   *
   * @param tripletIt iterable of triplets to be used for the construction of the graph
   * @return the constructed graph
   */
  public static Graph fromTriplets(Iterable<Triplet<Vertex, Edge>> tripletIt) {
    var graph = new Graph();
    for (var triplet : tripletIt) {
      graph.addVertex(triplet.getSourceVertex());
      graph.addVertex(triplet.getTargetVertex());
      graph.addEdge(triplet.getEdge());
    }
    return graph;
  }

  public Collection<Triplet<Vertex, Edge>> toTriplets() {
    var result = new ArrayList<Triplet<Vertex, Edge>>();
    for(var edge : edges){
      var source = vertexMap.get(edge.getSourceId());
      var target = vertexMap.get(edge.getTargetId());
      result.add(new Triplet(edge,source,target));
    }
    return result;
  }
}
