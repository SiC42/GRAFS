package edu.leipzig.grafs.operators.grouping.model;

import edu.leipzig.grafs.model.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.GradoopConstants;

/**
 * Data model that represents a vertex in the property graph model (with graph membership). This
 * class is used for aggregation of vertices, as we need to store which vertices have already been
 * aggregated.
 * <p>
 * Reason: <br> There may be vertices which are the source or target of multiple edges, hence they
 * may be multiple times in the stream.
 */
public class AggregatedVertex extends Vertex {

  private final GradoopIdSet aggregatedVertexIds;

  /**
   * Basic constructor which sets everything to default.
   */
  public AggregatedVertex() {
    super(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL, null, new GradoopIdSet());
    aggregatedVertexIds = new GradoopIdSet();
  }

  /**
   * Returns <tt>true</tt> if the given (vertex) ID is already registered as being aggregated.
   *
   * @param id ID for which should be checked if the corresponding vertex was aggregated
   * @return <tt>true</tt> if the given (vertex) ID is already registered as being aggregated
   */
  public boolean isAlreadyAggregated(GradoopId id) {
    return aggregatedVertexIds.contains(id);
  }

  /**
   * Returns <tt>true</tt> if the given vertex is already registered as being aggregated.
   *
   * @param vertex vertex for which should be checked if it was aggregated
   * @return <tt>true</tt> if the given vertex is already registered as being aggregated
   */
  public boolean isAlreadyAggregated(Vertex vertex) {
    return isAlreadyAggregated(vertex.getId());
  }

  /**
   * Adds the given (vertex) ID to the already aggregated vertices.
   *
   * @param id ID of the vertex which should be counted as already aggregated
   */
  public boolean addVertex(GradoopId id) {
    return aggregatedVertexIds.add(id);
  }

  /**
   * Adds the given vertex to the already aggregated vertices.
   *
   * @param vertex vertex which should be counted as already aggregated
   */
  public boolean addVertex(Vertex vertex) {
    return addVertex(vertex.getId());
  }

  /**
   * Adds the given (vertex) IDs to the already aggregated vertices.
   *
   * @param ids IDs of the vertices which should be counted as already aggregated
   */
  public void addVertices(GradoopIdSet ids) {
    aggregatedVertexIds.addAll(ids);
  }

  /**
   * Returns the IDs of all vertices which are already aggregated
   *
   * @return the IDs of all vertices which are already aggregated
   */
  public GradoopIdSet getAggregatedVertexIds() {
    return aggregatedVertexIds;
  }


}
