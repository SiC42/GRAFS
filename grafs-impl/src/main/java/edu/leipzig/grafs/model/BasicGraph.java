package edu.leipzig.grafs.model;

import edu.leipzig.grafs.exceptions.VertexNotPartOfTheGraphException;
import edu.leipzig.grafs.operators.matching.model.QueryEdge;
import edu.leipzig.grafs.operators.matching.model.QueryVertex;
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

public class BasicGraph<V extends Vertex, E extends Edge> implements Serializable {

    protected Collection<V> vertices;
    protected Map<GradoopId, V> vertexMap;
    protected Collection<E> edges;
    protected MultiMap<GradoopId, E> sourceToEdgeMap;
    protected MultiMap<GradoopId, E> targetToEdgeMap;
    private GradoopId id;

    /**
     * Constructs an empty graph.
     */
    public BasicGraph() {
      this(new HashSet<>(), new HashSet<>());
    }

    /**
     * Constructs a new graph (with a new ID) with the given vertices and edges.
     *
     * @param vertices vertices that should be part of the graph
     * @param edges    edges that should be part of the graph
     */
    public BasicGraph(Collection<V> vertices, Collection<E> edges) {
      this(GradoopId.get(), vertices, edges);
    }

    /**
     * Constructs a new graph properties.
     *
     * @param vertices vertices that should be part of the graph
     * @param edges    edges that should be part of the graph
     * @param graphId  ID to be used for the graph
     */
    public BasicGraph(GradoopId graphId, Collection<V> vertices, Collection<E> edges) {
      this(graphId, new HashSet<>(), new HashSet<>(), vertices, edges);
    }

    /**
     * Constructs a new graph properties.
     *
     * @param vertices vertices that should be part of the graph
     * @param edges    edges that should be part of the graph
     * @param graphId  ID to be used for the graph
     */
    protected BasicGraph(GradoopId graphId, Collection<V> newVColl, Collection<E> newEColl, Collection<V> vertices, Collection<E> edges) {
      this.id = graphId;
      this.vertices = newVColl;
      this.edges = newEColl;
      initGraph(vertices, edges);
    }

    /**
     * Returns the ID of this graph.
     *
     * @return the ID of this graph
     */
    public GradoopId getId() {
      return id;
    }

    /**
     * Adds a vertex to the graph. Returns <tt>true</tt> if this vertex was not already part of the
     * graph. Does not add the vertex, if it is already part of the graph.
     *
     * @param vertex vertex to be added to this graph
     * @return <tt>true</tt>  if this graph did not already contain the specified vertex
     */
    public boolean addVertex(V vertex) {
      boolean isNewVertex = vertices.add(vertex);
      if (isNewVertex) {
        vertexMap.put(vertex.getId(), vertex);
      }
      return isNewVertex;
    }

    /**
     * Adds the given vertices to the graph. Returns <tt>true</tt> if at least one vertex was not
     * already part of the graph. Does not add the vertices that are already part of the graph.
     *
     * @param vertices vertices to be added to this graph
     * @return <tt>true</tt>  if at least one vertex was not part of the graph (and was therefore
     * added)
     */
    public boolean addVertices(Collection<V> vertices) {
      boolean addedNewVertex = false;
      for (var vertex : vertices) {
        if (addVertex(vertex)) {
          addedNewVertex = true;
        }
      }

      return addedNewVertex;
    }

    /**
     * Returns the vertices of this graph as a set.
     *
     * @return the vertices of this graph
     */
    public Collection<V> getVertices() {
      return vertices;
    }

    /**
     * Adds an edge to the graph. Returns <tt>true</tt> if this edge was not already part of the
     * graph. Does not add the edge, if it is already part of the graph.
     *
     * @param edge edge to be added to this graph
     * @return <tt>true</tt>  if this graph did not already contain the specified edge
     * @throws VertexNotPartOfTheGraphException if the source or target vertex of the edge are not
     *                                          part of the graph
     */
    public boolean addEdge(E edge) throws VertexNotPartOfTheGraphException {
      if (!vertexMap.containsKey(edge.getSourceId())) {
        throw new VertexNotPartOfTheGraphException(
            String.format("No source vertex with id %s found for edge %s", edge.getSourceId(), edge));
      }
      if (!vertexMap.containsKey(edge.getTargetId())) {
        throw new VertexNotPartOfTheGraphException(
            String.format("No target vertex found for edge %s", edge));
      }
      boolean isNewEdge = edges.add(edge);
      if (isNewEdge) {
        sourceToEdgeMap.put(edge.getSourceId(), edge);
        targetToEdgeMap.put(edge.getTargetId(), edge);
      }
      return isNewEdge;
    }

    /**
     * Adds the given edges to the graph. Returns <tt>true</tt> if at least one edge was not already
     * part of the graph. Does not add the edges that are already part of the graph.
     *
     * @param edges edges to be added to this graph
     * @return <tt>true</tt>  if at least one edge was not part of the graph (and was therefore
     * added)
     * @throws VertexNotPartOfTheGraphException gets thrown, if there is at least one edges for which
     *                                          the source or target vertex are not part of the graph
     */
    public boolean addEdges(Collection<E> edges) throws VertexNotPartOfTheGraphException {
      var addedNewEdge = false;
      for (var edge : edges) {
        if (addEdge(edge)) {
          addedNewEdge = true;
        }
      }
      return addedNewEdge;
    }

    /**
     * Returns the edges of the graph as a set.
     *
     * @return the edges of the graph
     */
    public Collection<E> getEdges() {
      return edges;
    }

    /**
     * Returns the source vertex for the given edge, or <tt>null</tt> if this graph does not contain
     * the vertex.
     *
     * @param edge edge for which the source vertex should be returned
     * @return the source vertex of the edge or <tt>null</tt> if this graph does not contain the
     * vertex.
     */
    public V getSourceForEdge(E edge) {
      return vertexMap.get(edge.getSourceId());
    }

    /**
     * Returns the target vertex for the given edge, or <tt>null</tt> if this graph does not contain
     * the vertex.
     *
     * @param edge edge for which the target vertex should be returned
     * @return the target vertex of the edge or <tt>null</tt> if this graph does not contain the
     * vertex.
     */
    public V getTargetForEdge(E edge) {
      return vertexMap.get(edge.getTargetId());
    }

    /**
     * Returns a set of all edges that have the given vertex as source vertex.
     * <p>
     * Returns an empty set if there are no edges.
     *
     * @param vertex source vertex for which the corresponding edges should be returned
     * @return a set of all edges that have the given vertex as source vertex
     */
    public Set<E> getEdgesForSource(V vertex) {
      if (sourceToEdgeMap.containsKey(vertex.getId())) {
        return sourceToEdgeMap.get(vertex.getId());
      }
      return Collections.emptySet();
    }

    /**
     * Returns a set of all edges that have the given vertex as target vertex.
     * <p>
     * Returns an empty set if there are no edges.
     *
     * @param vertex target vertex for which the corresponding edges should be returned
     * @return a set of all edges that have the given vertex as target vertex
     */
    public Set<E> getEdgesForTarget(V vertex) {
      if (targetToEdgeMap.containsKey(vertex.getId())) {
        return targetToEdgeMap.get(vertex.getId());
      }
      return Collections.emptySet();
    }

    /**
     * Returns a set of all vertices that the target vertex of edges, for which the given vertex is a
     * source vertex.
     * <p>
     * This method allows a direct approach to get the target vertices, instead of getting the edge
     * via {@link #getEdgesForSource(V)} and then getting the target vertex via {@link
     * #getTargetForEdge(Edge)}.
     *
     * @param vertex source vertex for which the corresponding target vertices should be returned
     * @return a set of all target vertices that have the given vertex as source vertex (i.e. are part
     * of an edge)
     */
    public Set<V> getTargetForSourceVertex(V vertex) {
      var edgesOfSource = sourceToEdgeMap.get(vertex.getId());
      if (edgesOfSource == null) {
        return new HashSet<>();
      }
      return edgesOfSource.stream()
          .map(E::getTargetId)
          .map(vertexMap::get)
          .collect(Collectors.toSet());
    }

    /**
     * Returns the edge that has the two given vertices as source and target vertex.
     *
     * @param sourceVertex source vertex for which the edge should be returned
     * @param targetVertex target vertex for which the edge should be returned
     * @return the edge that has the two given vertices as source and target vertex
     */
    @Nullable
    public E getEdgeForVertices(V sourceVertex, V targetVertex) {
      Set<E> edgeIntersection = new HashSet<>(sourceToEdgeMap.get(sourceVertex.getId()));
      edgeIntersection.retainAll(targetToEdgeMap.get(targetVertex.getId()));
      Iterator<E> iterator = edgeIntersection.iterator();
      return iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Returns the vertex-induced subgraph for the given vertices, i.e. a graph with the given
     * vertices (provided they are in this graph) and all edges, for which both, the source and target
     * vertex, are part of the subgraph.
     *
     * @param vertices vertices for which a vertex-induced subgraph should be returned for
     * @return the vertex-induced subgraph for the given vertices
     */
    public BasicGraph<V,E> getVertexInducedSubGraph(Collection<V> vertices) {
      var vInducedSubGraph = new BasicGraph<V,E>(vertices, new HashSet<>());
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

    /**
     * Initializes the graph.
     *
     * @param vertices vertices that should be part of the graph
     * @param edges    edges that should be part of the graph
     */
    private void initGraph(Collection<V> vertices, Collection<E> edges) {
      this.vertexMap = new HashMap<>();
      this.sourceToEdgeMap = new MultiMap<>();
      this.targetToEdgeMap = new MultiMap<>();
      addVertices(vertices);
      addEdges(edges);
    }

    @Override
    public String toString() {
      return "Graph@" + id + "{" +
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
      BasicGraph<?, ?> graph = (BasicGraph<?, ?>) o;
      return Objects.equals(vertices, graph.vertices) && Objects
          .equals(edges, graph.edges) && Objects.equals(id, graph.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(vertices, vertexMap, edges, sourceToEdgeMap, targetToEdgeMap);
    }

    private void writeObject(java.io.ObjectOutputStream out)
        throws IOException {
      out.writeObject(id);
      out.writeInt(vertices.size());
      for (var vertex : vertices) {
        out.writeObject(vertex);
      }
      out.writeInt(edges.size());
      for (var edge : edges) {
        out.writeObject(edge);
      }
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      var id = (GradoopId) in.readObject();
      int size = in.readInt();
      var vertices = new HashSet<V>();
      for (int i = 0; i < size; i++) {
        var vertex = (V) in.readObject();
        vertices.add(vertex);
      }
      size = in.readInt();
      var edges = new HashSet<E>();
      for (int i = 0; i < size; i++) {
        var edge = (E) in.readObject();
        edges.add(edge);
      }
      this.id = id;
      this.vertices = new HashSet<>();
      this.edges = new HashSet<>();
      initGraph(vertices, edges);
    }
  }