package streaming.factory;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import streaming.model.Edge;

/**
 * Factory for creating EPGM edge POJOs.
 */
public class EdgeFactory implements Serializable {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  public Edge createEdge(GradoopId sourceVertexId,
      GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), sourceVertexId, targetVertexId);
  }

  public Edge initEdge(final GradoopId id, final GradoopId sourceVertexId,
      final GradoopId targetVertexId) {
    return initEdge(id, GradoopConstants.DEFAULT_EDGE_LABEL, sourceVertexId,
        targetVertexId);
  }

  public Edge createEdge(String label, GradoopId sourceVertexId,
      GradoopId targetVertexId) {
    return initEdge(GradoopId.get(), label, sourceVertexId, targetVertexId);
  }

  public Edge initEdge(final GradoopId id, final String label,
      final GradoopId sourceVertexId, final GradoopId targetVertexId) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, null);
  }

  public Edge createEdge(String label, GradoopId sourceVertexId,
      GradoopId targetVertexId, Properties properties) {
    return initEdge(GradoopId.get(),
        label, sourceVertexId, targetVertexId, properties);
  }

  public Edge initEdge(
      GradoopId id,
      String label,
      GradoopId sourceVertexId,
      GradoopId targetVertexId,
      Properties properties) {

    return
        initEdge(id, label, sourceVertexId, targetVertexId, properties, null);
  }

  public Edge createEdge(String label, GradoopId sourceVertexId,
      GradoopId targetVertexId, GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(),
        label, sourceVertexId, targetVertexId, graphIds);
  }

  public Edge initEdge(final GradoopId id, final String label,
      final GradoopId sourceVertexId, final GradoopId targetVertexId,
      GradoopIdSet graphs) {
    return initEdge(id, label, sourceVertexId, targetVertexId, null, graphs);
  }

  public Edge createEdge(String label, GradoopId sourceVertexId,
      GradoopId targetVertexId, Properties properties,
      GradoopIdSet graphIds) {
    return initEdge(GradoopId.get(),
        label, sourceVertexId, targetVertexId, properties, graphIds);
  }

  public Edge initEdge(final GradoopId id, final String label,
      final GradoopId sourceVertexId, final GradoopId targetVertexId,
      final Properties properties, GradoopIdSet graphIds) {
    checkNotNull(id, "Identifier was null");
    checkNotNull(label, "Label was null");
    checkNotNull(sourceVertexId, "Source vertex id was null");
    checkNotNull(targetVertexId, "Target vertex id was null");
    return new Edge(id, label, sourceVertexId, targetVertexId,
        properties, graphIds);
  }

  public Class<Edge> getType() {
    return Edge.class;
  }
}
