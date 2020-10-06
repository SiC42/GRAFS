package edu.leipzig.grafs.factory;


import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Vertex;
import java.io.Serializable;
import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;

/**
 * Factory for creating {@link Edge}.
 */
public class EdgeFactory implements Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Creates an edge with the given information. WARNING: With this method the source and target
   * vertex will not be set and therefore, it may lead to exceptions.
   *
   * @return new edge
   */
  public static Edge createEdge() {
    return initEdge(GradoopId.get(), null, null);
  }

  /**
   * Creates an edge with the given information.
   *
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @return new edge
   */
  public static Edge createEdge(GradoopId sourceVertexId,
      GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), sourceVertexId, targetVertexId);
  }

  /**
   * Creates an edge with the given information.
   *
   * @param sourceVertex source vertex for the edge
   * @param targetVertex target vertex for the edge
   * @return new edge
   */
  public static Edge createEdge(Vertex sourceVertex,
      Vertex targetVertex) {
    return initEdge(GradoopId.get(), sourceVertex.getId(), targetVertex.getId());
  }

  /**
   * Initializes a new edge with the given information.
   *
   * @param id             ID for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @return new edge
   */
  public static Edge initEdge(final GradoopId id, final GradoopId sourceVertexId,
      final GradoopId targetVertexId) {
    return initEdge(id, GradoopConstants.DEFAULT_EDGE_LABEL, sourceVertexId,
        targetVertexId);
  }

  /**
   * Creates an edge with the given information.
   *
   * @param label          label for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @return new edge
   */
  public static Edge createEdge(String label, GradoopId sourceVertexId,
      GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId);
  }

  /**
   * Initializes a new edge with the given information.
   *
   * @param id             ID for the edge
   * @param label          label for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @return new edge
   */
  public static Edge initEdge(final GradoopId id, final String label,
      final GradoopId sourceVertexId, final GradoopId targetVertexId) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, new GradoopIdSet());
  }

  /**
   * Creates an edge with the given information.
   *
   * @param label          label for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @param properties     properties for the edge
   * @return new edge
   */
  public static Edge createEdge(String label, GradoopId sourceVertexId,
      GradoopId targetVertexId, Properties properties) {
    return initEdge(GradoopId.get(),
        label, sourceVertexId, targetVertexId, properties);
  }

  /**
   * Initializes a new edge with the given information.
   *
   * @param id             ID for the edge
   * @param label          label for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @param properties     properties for the edge
   * @return new edge
   */
  public static Edge initEdge(
      GradoopId id,
      String label,
      GradoopId sourceVertexId,
      GradoopId targetVertexId,
      Properties properties) {

    return
        initEdge(id, label, sourceVertexId, targetVertexId, properties, new GradoopIdSet());
  }

  /**
   * Creates an edge with the given information.
   *
   * @param label          label for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @param graphIds       graph ids for the edge
   * @return new edge
   */
  public static Edge createEdge(String label, GradoopId sourceVertexId,
      GradoopId targetVertexId, GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(),
        label, sourceVertexId, targetVertexId, graphIds);
  }

  /**
   * Initializes a new edge with the given information.
   *
   * @param id             ID for the edge
   * @param label          label for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @param graphs
   * @return new edge
   */
  public static Edge initEdge(final GradoopId id, final String label,
      final GradoopId sourceVertexId, final GradoopId targetVertexId,
      GradoopIdSet graphs) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, graphs);
  }

  /**
   * Creates an edge with the given information.
   *
   * @param label          label for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @param properties     properties for the edge
   * @param graphIds       graph ids for the edge
   * @return new edge
   */
  public static Edge createEdge(String label, GradoopId sourceVertexId,
      GradoopId targetVertexId, Properties properties,
      GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(),
        label, sourceVertexId, targetVertexId, properties, graphIds);
  }

  /**
   * Initializes a new edge with the given information.
   *
   * @param id             ID for the edge
   * @param label          label for the edge
   * @param sourceVertexId ID of the source vertex for the edge
   * @param targetVertexId ID of the target vertex for the edge
   * @param properties     properties for the edge
   * @param graphIds       graph ids for the edge
   * @return new edge
   */
  public static Edge initEdge(final GradoopId id, final String label,
      final GradoopId sourceVertexId, final GradoopId targetVertexId,
      final Properties properties, GradoopIdSet graphIds) {
    Objects.requireNonNull(id, "Identifier was null");
    Objects.requireNonNull(label, "Label was null");
    return new Edge(id, label, sourceVertexId, targetVertexId,
        properties, graphIds);
  }

  /**
   * Creates an edge with the given information of another edge.
   *
   * @param edge edge which should be copied
   * @return new edge copy
   */
  public static Edge createEdge(Edge edge) {
    return initEdge(edge.getId(),
        edge.getLabel(),
        edge.getSourceId(),
        edge.getTargetId(),
        edge.getProperties(),
        edge.getGraphIds());
  }
}
