package edu.leipzig.grafs.factory;

import edu.leipzig.grafs.model.Vertex;
import java.io.Serializable;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

public class VertexFactory implements Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;


  public static Vertex createVertex() {
    return initVertex(GradoopId.get());
  }


  public static Vertex initVertex(final GradoopId vertexID) {
    return initVertex(vertexID, GradoopConstants.DEFAULT_VERTEX_LABEL, null, new GradoopIdSet());
  }


  public static Vertex createVertex(String label) {
    return initVertex(GradoopId.get(), label);
  }


  public static Vertex initVertex(final GradoopId vertexID, final String label) {
    return initVertex(vertexID, label, null, new GradoopIdSet());
  }


  public static Vertex createVertex(String label, Properties properties) {
    return initVertex(GradoopId.get(), label, properties);
  }


  public static Vertex initVertex(final GradoopId vertexID, final String label,
      Properties properties) {
    return initVertex(vertexID, label, properties, null);
  }


  public static Vertex createVertex(String label, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, graphIds);
  }


  public static Vertex initVertex(final GradoopId vertexID, final String label,
      final GradoopIdSet graphs) {
    return initVertex(vertexID, label, null, graphs);
  }


  public static Vertex createVertex(String label, Properties properties,
      GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, properties, graphIds);
  }


  public static Vertex initVertex(final GradoopId id, final String label,
      final Properties properties, final GradoopIdSet graphs) {
    Objects.requireNonNull(id, "Identifier was null");
    Objects.requireNonNull(label, "Label was null");
    return new Vertex(id, label, properties, graphs);
  }

  public static Vertex createVertex(Vertex vertex) {
    return initVertex(vertex.getId(),
        vertex.getLabel(),
        vertex.getProperties(),
        vertex.getGraphIds());
  }


  public static Class<Vertex> getType() {
    return Vertex.class;
  }
}

