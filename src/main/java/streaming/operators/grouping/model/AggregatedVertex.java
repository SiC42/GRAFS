package streaming.operators.grouping.model;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.GradoopConstants;
import streaming.model.Vertex;

public class AggregatedVertex extends Vertex {

  private final GradoopIdSet vertexIds;

  public AggregatedVertex() {
    super(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL, null, new GradoopIdSet());
    vertexIds = new GradoopIdSet();
  }

  public boolean isAlreadyAggregated(GradoopId id) {
    return vertexIds.contains(id);
  }

  public boolean isAlreadyAggregated(Vertex vertex) {
    return isAlreadyAggregated(vertex.getId());
  }

  public boolean addVertex(GradoopId id) {
    return vertexIds.add(id);
  }

  public boolean addVertex(Vertex vertex) {
    return addVertex(vertex.getId());
  }

}
