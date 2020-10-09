package edu.leipzig.grafs.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.model.Vertex;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.GDLHandler.Builder;
import org.s1ck.gdl.model.Graph;
import org.s1ck.gdl.model.GraphElement;

public class AsciiGraphLoader {

  /**
   * Used to parse GDL scripts.
   */
  private final GDLHandler gdlHandler;

  /**
   * Mapping between GDL ids and Gradoop IDs.
   */
  private final Map<Long, GradoopId> graphIds;
  /**
   * Stores graphs that are assigned to a variable.
   */
  private final Map<String, GradoopId> graphIdCache;

  /**
   * Stores all vertices contained in the GDL script.
   */
  private final Map<GradoopId, Vertex> vertices;
  /**
   * Mapping between GDL ids and Gradoop IDs.
   */
  private final Map<Long, GradoopId> vertexIds;
  /**
   * Stores vertices that are assigned to a variable.
   */
  private final Map<String, Vertex> vertexCache;

  /**
   * Stores all edges contained in the GDL script.
   */
  private final Map<GradoopId, Edge> edges;
  /**
   * Mapping between GDL ids and Gradoop IDs.
   */
  private final Map<Long, GradoopId> edgeIds;
  /**
   * Stores edges that are assigned to a variable.
   */
  private final Map<String, Edge> edgeCache;

  public AsciiGraphLoader(GDLHandler gdlHandler) {
    this.gdlHandler = gdlHandler;
    this.graphIds = new HashMap<>();
    this.graphIdCache = new HashMap<>();
    this.vertices = new HashMap<>();
    this.vertexIds = new HashMap<>();
    this.vertexCache = new HashMap<>();
    this.edges = new HashMap<>();
    this.edgeIds = new HashMap<>();
    this.edgeCache = new HashMap<>();
    init();
  }


  public static AsciiGraphLoader fromFile(String fileName) throws IOException {
    GDLHandler gdlHandler = createDefaultGdlHandlerBuilder()
        .buildFromFile(fileName);
    return new AsciiGraphLoader(gdlHandler);
  }

  public static AsciiGraphLoader fromInputStream(InputStream graphStream) throws IOException {
    GDLHandler gdlHandler = createDefaultGdlHandlerBuilder()
        .buildFromStream(graphStream);
    return new AsciiGraphLoader(gdlHandler);
  }

  public static AsciiGraphLoader fromString(String graphStr) {
    GDLHandler gdlHandler = createDefaultGdlHandlerBuilder().buildFromString(graphStr);
    return new AsciiGraphLoader(gdlHandler);
  }

  private static Builder createDefaultGdlHandlerBuilder() {
    return new GDLHandler.Builder()
        .setDefaultGraphLabel(GradoopConstants.DEFAULT_GRAPH_LABEL)
        .setDefaultVertexLabel(GradoopConstants.DEFAULT_VERTEX_LABEL)
        .setDefaultEdgeLabel(GradoopConstants.DEFAULT_EDGE_LABEL);
  }

  /**
   * Appends the given ASCII GDL to the graph handled by that loader.
   * <p>
   * Variables that were previously used, can be reused in the given script and refer to the same
   * entities.
   *
   * @param asciiGraph GDL string
   */
  public void appendFromString(String asciiGraph) {
    this.gdlHandler.append(asciiGraph);
    init();
  }

  // ---------------------------------------------------------------------------
  //  EdgeCollection and EdgeStream methods
  // ---------------------------------------------------------------------------

  public EdgeStream createEdgeStreamByGraphVariables(FlinkConfig config, String... expected) {
    return createEdgeStream(config, createEdgeContainersByGraphVariables(expected));
  }

  public EdgeStream createEdgeStream(FlinkConfig config) {
    return createEdgeStream(config, createEdgeContainers());
  }

  private EdgeStream createEdgeStream(FlinkConfig config, Collection<EdgeContainer> edgeContainer) {
    StreamExecutionEnvironment env = config.getExecutionEnvironment();
    DataStream<EdgeContainer> stream = env.fromCollection(edgeContainer);
    return new EdgeStream(stream, config);
  }

  public Collection<EdgeContainer> createEdgeContainersByGraphVariables(String... expected) {
    var edges = getEdgesByGraphVariables(expected);
    return createEdgeContainers(edges);
  }

  public Collection<EdgeContainer> createEdgeContainers() {
    return createEdgeContainers(edges.values());
  }

  private Collection<EdgeContainer> createEdgeContainers(Collection<Edge> edges) {
    Set<EdgeContainer> edgeContainers = new HashSet<>();
    for (var edge : edges) {
      var source = vertices.get(edge.getSourceId());
      var target = vertices.get(edge.getTargetId());
      EdgeContainer ec = new EdgeContainer(edge, source, target);
      edgeContainers.add(ec);
    }
    return edgeContainers;
  }

  // ---------------------------------------------------------------------------
  //  Graph methods
  // ---------------------------------------------------------------------------


  public edu.leipzig.grafs.model.Graph createGraphByGraphVariables(String... expected) {
    var vertices = getVerticesByGraphVariables(expected);
    var edges = getEdgesByGraphVariables(expected);
    return createGraph(vertices, edges);
  }

  public edu.leipzig.grafs.model.Graph createGraph() {
    return createGraph(vertices.values(), edges.values());
  }

  private edu.leipzig.grafs.model.Graph createGraph(Collection<Vertex> vertices,
      Collection<Edge> edges) {
    return new edu.leipzig.grafs.model.Graph(vertices, edges);
  }

  // ---------------------------------------------------------------------------
  //  Graph methods
  // ---------------------------------------------------------------------------

  /**
   * Returns all GraphHeads contained in the ASCII graph.
   *
   * @return graphHeads
   */
  public Collection<GradoopId> getGraphIds() {
    return new ImmutableSet.Builder<GradoopId>()
        .addAll(graphIds.values()).build();
  }

  /**
   * Returns GraphHead by given variable.
   *
   * @param variable variable used in GDL script
   * @return graphHead or {@code null} if graph is not cached
   */
  public GradoopId getGraphIdByVariable(String variable) {
    return getGraphIdCache().get(variable);
  }

  /**
   * Returns GraphHeads by their given variables.
   *
   * @param variables variables used in GDL script
   * @return graphHeads that are assigned to the given variables
   */
  public Collection<GradoopId> getGraphIdsByVariables(String... variables) {
    Collection<GradoopId> result =
        Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      GradoopId graphId = getGraphIdByVariable(variable);
      if (graphId != null) {
        result.add(graphId);
      }
    }
    return result;
  }

  /**
   * Returns all graph heads that are bound to a variable in the GDL script.
   *
   * @return variable to graphHead mapping
   */
  public Map<String, GradoopId> getGraphIdCache() {
    return new ImmutableMap.Builder<String, GradoopId>().putAll(graphIdCache)
        .build();
  }

  // ---------------------------------------------------------------------------
  //  Vertex methods
  // ---------------------------------------------------------------------------

  /**
   * Returns all vertices contained in the ASCII graph.
   *
   * @return vertices
   */
  public Collection<Vertex> getVertices() {
    return new ImmutableSet.Builder<Vertex>().addAll(vertices.values()).build();
  }

  /**
   * Returns vertex by its given variable.
   *
   * @param variable variable used in GDL script
   * @return vertex or {@code null} if not present
   */
  public Vertex getVertexByVariable(String variable) {
    return vertexCache.get(variable);
  }

  /**
   * Returns vertices by their given variables.
   *
   * @param variables variables used in GDL script
   * @return vertices
   */
  public Collection<Vertex> getVerticesByVariables(String... variables) {
    Collection<Vertex> result = Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      Vertex vertex = getVertexByVariable(variable);
      if (vertex != null) {
        result.add(vertex);
      }
    }
    return result;
  }

  /**
   * Returns all vertices that belong to the given graphs.
   *
   * @param graphIds graph identifiers
   * @return vertices that are contained in the graphs
   */
  public Collection<Vertex> getVerticesByGraphIds(GradoopIdSet graphIds) {
    Collection<Vertex> result = Sets.newHashSetWithExpectedSize(graphIds.size());
    for (Vertex vertex : vertices.values()) {
      if (vertex.getGraphIds().containsAny(graphIds)) {
        result.add(vertex);
      }
    }
    return result;
  }

  /**
   * Returns all vertices that belong to the given graph variables.
   *
   * @param graphVariables graph variables used in the GDL script
   * @return vertices that are contained in the graphs
   */
  public Collection<Vertex> getVerticesByGraphVariables(String... graphVariables) {
    GradoopIdSet graphIds = new GradoopIdSet();
    graphIds.addAll(getGraphIdsByVariables(graphVariables));
    return getVerticesByGraphIds(graphIds);
  }

  /**
   * Returns all vertices that are bound to a variable in the GDL script.
   *
   * @return variable to vertex mapping
   */
  public Map<String, Vertex> getVertexCache() {
    return new ImmutableMap.Builder<String, Vertex>().putAll(vertexCache).build();
  }

  // ---------------------------------------------------------------------------
  //  Edge methods
  // ---------------------------------------------------------------------------

  /**
   * Returns all edges contained in the ASCII graph.
   *
   * @return edges
   */
  public Collection<Edge> getEdges() {
    return new ImmutableSet.Builder<Edge>().addAll(edges.values()).build();
  }

  /**
   * Returns edge by its given variable.
   *
   * @param variable variable used in GDL script
   * @return edge or {@code null} if not present
   */
  public Edge getEdgeByVariable(String variable) {
    return edgeCache.get(variable);
  }

  /**
   * Returns edges by their given variables.
   *
   * @param variables variables used in GDL script
   * @return edges
   */
  public Collection<Edge> getEdgesByVariables(String... variables) {
    Collection<Edge> result = Sets.newHashSetWithExpectedSize(variables.length);
    for (String variable : variables) {
      Edge edge = edgeCache.get(variable);
      if (edge != null) {
        result.add(edge);
      }
    }
    return result;
  }

  /**
   * Returns all edges that belong to the given graphs.
   *
   * @param graphIds Graph identifiers
   * @return edges
   */
  public Collection<Edge> getEdgesByGraphIds(GradoopIdSet graphIds) {
    Collection<Edge> result = Sets.newHashSetWithExpectedSize(graphIds.size());
    for (Edge edge : edges.values()) {
      if (edge.getGraphIds().containsAny(graphIds)) {
        result.add(edge);
      }
    }
    return result;
  }

  /**
   * Returns all edges that belong to the given graph variables.
   *
   * @param variables graph variables used in the GDL script
   * @return edges
   */
  public Collection<Edge> getEdgesByGraphVariables(String... variables) {
    GradoopIdSet graphIds = new GradoopIdSet();
    graphIds.addAll(getGraphIdsByVariables(variables));
    return getEdgesByGraphIds(graphIds);
  }

  /**
   * Returns all edges that are bound to a variable in the GDL script.
   *
   * @return variable to edge mapping
   */
  public Map<String, Edge> getEdgeCache() {
    return new ImmutableMap.Builder<String, Edge>().putAll(edgeCache).build();
  }

  // ---------------------------------------------------------------------------
  //  Private init methods
  // ---------------------------------------------------------------------------

  /**
   * Initializes the AsciiGraphLoader
   */
  private void init() {
    initGraphIds();
    initVertices();
    initEdges();
  }

  /**
   * Initializes GraphHeads and their cache.
   */
  private void initGraphIds() {
    for (var graph : gdlHandler.getGraphs()) {
      if (!graphIds.containsKey(graph.getId())) {
        initGraphHead(graph);
      }
    }
    for (Map.Entry<String, org.s1ck.gdl.model.Graph> e : gdlHandler.getGraphCache().entrySet()) {
      updateGraphCache(e.getKey(), e.getValue());
    }
  }

  /**
   * Creates a new Graph from the GDL Loader.
   *
   * @param g graph from GDL Loader
   */
  private void initGraphHead(Graph g) {
    GradoopId graphId = GradoopId.get();
    graphIds.put(g.getId(), graphId);
  }

  /**
   * Updates the graph cache.
   *
   * @param variable graph variable used in GDL script
   * @param g        graph from GDL loader
   */
  private void updateGraphCache(String variable, Graph g) {
    graphIdCache.put(variable, graphIds.get(g.getId()));
  }

  /**
   * Initializes vertices and their cache.
   */
  private void initVertices() {
    for (var v : gdlHandler.getVertices()) {
      initVertex(v);
    }

    for (Map.Entry<String, org.s1ck.gdl.model.Vertex> e : gdlHandler.getVertexCache().entrySet()) {
      updateVertexCache(e.getKey(), e.getValue());
    }
  }

  /**
   * Updates the vertex cache.
   *
   * @param variable vertex variable used in GDL script
   * @param v        vertex from GDL loader
   */
  private void updateVertexCache(String variable, org.s1ck.gdl.model.Vertex v) {
    vertexCache.put(variable, vertices.get(vertexIds.get(v.getId())));
  }

  /**
   * Creates a new Vertex from the GDL Loader or updates an existing one.
   *
   * @param v vertex from GDL Loader
   */
  private void initVertex(org.s1ck.gdl.model.Vertex v) {
    Vertex vertex;
    if (!vertexIds.containsKey(v.getId())) {
      vertex = VertexFactory.createVertex(
          v.getLabel(),
          Properties.createFromMap(v.getProperties()),
          createGradoopIdSet(v));
      vertexIds.put(v.getId(), vertex.getId());
      vertices.put(vertex.getId(), vertex);
    } else {
      vertex = vertices.get(vertexIds.get(v.getId()));
      vertex.setGraphIds(createGradoopIdSet(v));
    }
  }

  /**
   * Initializes edges and their cache.
   */
  private void initEdges() {
    for (org.s1ck.gdl.model.Edge e : gdlHandler.getEdges()) {
      initEdge(e);
    }

    for (Map.Entry<String, org.s1ck.gdl.model.Edge> e : gdlHandler.getEdgeCache().entrySet()) {
      updateEdgeCache(e.getKey(), e.getValue());
    }
  }


  /**
   * Creates a new Edge from the GDL Loader.
   *
   * @param e edge from GDL loader
   * @return edge
   */
  private Edge initEdge(org.s1ck.gdl.model.Edge e) {
    Edge edge;
    if (!edgeIds.containsKey(e.getId())) {
      edge = EdgeFactory.createEdge(
          e.getLabel(),
          vertexIds.get(e.getSourceVertexId()),
          vertexIds.get(e.getTargetVertexId()),
          Properties.createFromMap(e.getProperties()),
          createGradoopIdSet(e));
      edgeIds.put(e.getId(), edge.getId());
      edges.put(edge.getId(), edge);
    } else {
      edge = edges.get(edgeIds.get(e.getId()));
      edge.setGraphIds(createGradoopIdSet(e));
    }
    return edge;
  }

  /**
   * Updates the edge cache.
   *
   * @param variable edge variable used in the GDL script
   * @param e        edge from GDL loader
   */
  private void updateEdgeCache(String variable, org.s1ck.gdl.model.Edge e) {
    edgeCache.put(variable, edges.get(edgeIds.get(e.getId())));
  }

  /**
   * Creates a {@code GradoopIDSet} from the long identifiers stored at the given graph element.
   *
   * @param e graph element
   * @return GradoopIDSet for the given element
   */
  private GradoopIdSet createGradoopIdSet(GraphElement e) {
    GradoopIdSet result = new GradoopIdSet();
    for (Long graphId : e.getGraphs()) {
      result.add(graphIds.get(graphId));
    }
    return result;
  }
}
