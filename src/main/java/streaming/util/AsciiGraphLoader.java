package streaming.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.GDLHandler.Builder;
import org.s1ck.gdl.model.Graph;
import org.s1ck.gdl.model.GraphElement;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Vertex;

public class AsciiGraphLoader {

  private final GDLHandler gdlHandler;
  private final Map<Long, GradoopId> graphIds;
  private final Map<Long, Vertex> vertices;
  private final Map<Long, Edge> edges;
  private final Set<EdgeContainer> edgeContainers;

  private AsciiGraphLoader(GDLHandler gdlHandler) {
    this.gdlHandler = gdlHandler;
    this.graphIds = new HashMap<>();
    this.vertices = new HashMap<>();
    this.edges = new HashMap<>();
    this.edgeContainers = new HashSet<>();
    initGraphIds();
    initVertices();
    buildEdgeContainers();
  }

  public static AsciiGraphLoader fromFile(String fileName) throws IOException {
    GDLHandler gdlHandler = createDefaultGdlHandlerBuilder()
        .buildFromFile(fileName);
    return new AsciiGraphLoader(gdlHandler);
  }

  public static AsciiGraphLoader fromStream(InputStream graphStream) throws IOException {
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

  public Collection<Vertex> getVertices() {
    return Collections.unmodifiableCollection(vertices.values());
  }

  public Collection<Edge> getEdges() {
    return Collections.unmodifiableCollection(edges.values());
  }

  public Collection<EdgeContainer> getEdgeContainers() {
    return Collections.unmodifiableCollection(edgeContainers);
  }

  public Collection<GradoopId> getGraphIds() {
    return graphIds.values();
  }

  private void buildEdgeContainers() {
    for (org.s1ck.gdl.model.Edge gdlE : gdlHandler.getEdges()) {
      EdgeContainer e = createEdgeContainerFromGdl(gdlE);
      edgeContainers.add(e);
    }
  }

  private void initGraphIds() {
    for (Graph g : gdlHandler.getGraphs()) {
      graphIds.put(g.getId(), GradoopId.get());
    }
  }

  private void initVertices() {
    for (org.s1ck.gdl.model.Vertex v : gdlHandler.getVertices()) {
      vertices.put(v.getId(), createVertexFromGdl(v));
    }
  }

  private EdgeContainer createEdgeContainerFromGdl(org.s1ck.gdl.model.Edge gdlE) {
    Vertex sourceV = vertices.get(gdlE.getSourceVertexId());
    Vertex targetV = vertices.get(gdlE.getTargetVertexId());
    Edge edge = createEdgeFromGdl(gdlE, sourceV.getId(), targetV.getId());
    edges.put(gdlE.getId(), edge);
    return new EdgeContainer(edge, sourceV, targetV);
  }

  private Vertex createVertexFromGdl(org.s1ck.gdl.model.Vertex gdlVertex) {
    Properties properties = new Properties();
    for (Map.Entry<String, Object> prop : gdlVertex.getProperties().entrySet()) {
      properties.set(prop.getKey(), prop.getValue());
    }
    return new Vertex(GradoopId.get(), gdlVertex.getLabel(), properties, createGradoopIdSet(gdlVertex));
  }

  /**
   * Creates a {@code GradoopIDSet} from the long identifiers stored at the
   * given graph element.
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

  private Edge createEdgeFromGdl(org.s1ck.gdl.model.Edge gdlE, GradoopId sourceId, GradoopId targetId) {
    Properties properties = new Properties();
    for (Map.Entry<String, Object> prop : gdlE.getProperties().entrySet()) {
      properties.set(prop.getKey(), prop.getValue());
    }
    return new Edge(GradoopId.get(), gdlE.getLabel(), sourceId, targetId, properties, createGradoopIdSet(gdlE));
  }

}
