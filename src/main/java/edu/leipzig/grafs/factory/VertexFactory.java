package edu.leipzig.grafs.factory;

import edu.leipzig.grafs.model.Vertex;
import java.io.Serializable;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

/**
 * Factory for creating {@link Vertex}.
 */
public class VertexFactory implements Serializable {

  private static final long serialVersionUID = 42L;

  /**
   * Creates a vertex with the given information.
   *
   * @return a vertex
   */
  public static Vertex createVertex() {
    return initVertex(GradoopId.get());
  }

  /**
   * Initializes a new vertex with the given information.
   *
   * @param id ID of the vertex
   * @return a vertex
   */
  public static Vertex initVertex(final GradoopId id) {
    return initVertex(id, GradoopConstants.DEFAULT_VERTEX_LABEL, null, new GradoopIdSet());
  }

  /**
   * Creates a vertex with the given information.
   *
   * @param label label of the vertex
   * @return a vertex
   */
  public static Vertex createVertex(String label) {
    return initVertex(GradoopId.get(), label);
  }

  /**
   * Initializes a new vertex with the given information.
   *
   * @param id    ID of the vertex
   * @param label label of the vertex
   * @return a vertex
   */
  public static Vertex initVertex(final GradoopId id, final String label) {
    return initVertex(id, label, null, new GradoopIdSet());
  }

  /**
   * Creates a vertex with the given information.
   *
   * @param label
   * @param properties properties for the vertex
   * @return a vertex
   */
  public static Vertex createVertex(String label, Properties properties) {
    return initVertex(GradoopId.get(), label, properties);
  }

  /**
   * Initializes a new vertex with the given information.
   *
   * @param id         ID of the vertex
   * @param label      label of the vertex
   * @param properties properties for the vertex
   * @return a vertex
   */
  public static Vertex initVertex(final GradoopId id, final String label,
      Properties properties) {
    return initVertex(id, label, properties, null);
  }

  /**
   * Creates a vertex with the given information.
   *
   * @param label    label of the vertex
   * @param graphIds graph ids for the vertex
   * @return a vertex
   */
  public static Vertex createVertex(String label, GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, graphIds);
  }

  /**
   * Initializes a new vertex with the given information.
   *
   * @param vertexID id for the vertex
   * @param label    label of the vertex
   * @param graphIds graph ids for the vertex
   * @return a vertex
   */
  public static Vertex initVertex(final GradoopId vertexID, final String label,
      final GradoopIdSet graphIds) {
    return initVertex(vertexID, label, null, graphIds);
  }

  /**
   * Creates a vertex with the given information.
   *
   * @param label      label of the vertex
   * @param properties properties for the vertex
   * @param graphIds   graph ids for the vertex
   * @return a vertex
   */
  public static Vertex createVertex(String label, Properties properties,
      GradoopIdSet graphIds) {
    return initVertex(GradoopId.get(), label, properties, graphIds);
  }

  /**
   * Initializes a new vertex with the given information.
   *
   * @param id         ID of the vertex
   * @param label      label of the vertex
   * @param properties properties for the vertex
   * @param graphIds   graph ids for the vertex
   * @return a vertex
   */
  public static Vertex initVertex(final GradoopId id, final String label,
      final Properties properties, final GradoopIdSet graphIds) {
    Objects.requireNonNull(id, "Identifier was null");
    Objects.requireNonNull(label, "Label was null");
    return new Vertex(id, label, properties, graphIds);
  }

  /**
   * Creates a vertex with the given information.
   *
   * @param vertex
   * @return a vertex
   */
  public static Vertex createVertex(Vertex vertex) {
    return initVertex(vertex.getId(),
        vertex.getLabel(),
        vertex.getProperties(),
        vertex.getGraphIds());
  }
}

