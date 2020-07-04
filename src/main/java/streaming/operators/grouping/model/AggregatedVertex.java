package streaming.operators.grouping.model;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.GradoopConstants;
import streaming.model.Vertex;

public class AggregatedVertex extends Vertex {

  private final GradoopIdSet aggregatedVertexIds;

  public AggregatedVertex() {
    super(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL, null, new GradoopIdSet());
    aggregatedVertexIds = new GradoopIdSet();
  }

  public boolean isAlreadyAggregated(GradoopId id) {
    return aggregatedVertexIds.contains(id);
  }

  public boolean isAlreadyAggregated(Vertex vertex) {
    return isAlreadyAggregated(vertex.getId());
  }

  public boolean addVertex(GradoopId id) {
    return aggregatedVertexIds.add(id);
  }

  public boolean addVertex(Vertex vertex) {
    return addVertex(vertex.getId());
  }

  public void addVertices(GradoopIdSet ids) {
    aggregatedVertexIds.addAll(ids);
  }

  public GradoopIdSet getAggregatedVertexIds() {
    return aggregatedVertexIds;
  }


}
