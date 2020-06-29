package streaming.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Graph;
import org.s1ck.gdl.model.GraphElement;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.Vertex;

public class AsciiGraphLoader {


  GDLHandler gdlHandler;

  private final Map<Long, GradoopId> graphIds;
  private final Map<Long, Vertex> vertices;

  public AsciiGraphLoader(){
    this.graphIds = new HashMap<>();
    this.vertices = new HashMap<>();
  }


  public Collection<EdgeContainer> loadFromString(String graphStr) {
    gdlHandler = new GDLHandler.Builder().buildFromString(graphStr);
    initGraphIds();
    Set<EdgeContainer> edges = new HashSet<>();
    initVertices();

    for (org.s1ck.gdl.model.Edge gdlE : gdlHandler.getEdges()) {
      EdgeContainer e = createEdgeContainerFromGdl(gdlE);
      edges.add(e);
    }
    return edges;
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
    return new EdgeContainer(edge, sourceV, targetV);
  }

  private Vertex createVertexFromGdl(org.s1ck.gdl.model.Vertex gdlVertex) {
    HashMap<String, String> properties = new HashMap<>();
    for (Map.Entry<String, Object> prop : gdlVertex.getProperties().entrySet()) {
      properties.put(prop.getKey(), prop.getValue().toString());
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
    HashMap<String, String> properties = new HashMap<>();
    for (Map.Entry<String, Object> prop : gdlE.getProperties().entrySet()) {
      properties.put(prop.getKey(), prop.getValue().toString());
    }
    return new Edge(GradoopId.get(), gdlE.getLabel(), sourceId, targetId, properties, createGradoopIdSet(gdlE));
  }

}
