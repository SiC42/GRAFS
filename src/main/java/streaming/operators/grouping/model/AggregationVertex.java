package streaming.operators.grouping.model;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.GradoopConstants;
import streaming.model.Vertex;

public class AggregationVertex extends Vertex {

  private GradoopIdSet vertexIds;

  public AggregationVertex(){
    super(GradoopId.get(), GradoopConstants.DEFAULT_VERTEX_LABEL, null, new GradoopIdSet());
  }

  public GradoopIdSet getIdsOfAggregatedVertices(){
    return vertexIds;
  }

  public boolean isAlreadyAggregated(Vertex vertex){
    return vertexIds.contains(vertex.getId());
  }

  public void addVertex(Vertex vertex){
    vertexIds.add(vertex.getId());
  }

}
