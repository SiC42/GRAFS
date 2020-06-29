package streaming.model;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

public abstract class GraphElement extends Element {

  private GradoopIdSet graphIds;

  /**
   * Default constructor.
   */
  public GraphElement() {
    super();
  }

  /**
   * Creates an EPGM graph element using the given arguments.
   *
   * @param id         element id
   * @param label      element label
   * @param properties element properties
   * @param graphIds   graphIds that element is contained in
   */
  protected GraphElement(GradoopId id, String label,
      Properties properties, GradoopIdSet graphIds) {
    super(id, label, properties);
    this.graphIds = graphIds;
  }


  public GradoopIdSet getGraphIds() {
    return graphIds;
  }

  public void setGraphIds(GradoopIdSet graphIds) {
    this.graphIds = graphIds;
  }

  public void addGraphId(GradoopId graphId) {
    if (graphIds == null) {
      graphIds = new GradoopIdSet();
    }
    graphIds.add(graphId);
  }

  public void resetGraphIds() {
    if (graphIds != null) {
      graphIds.clear();
    }
  }


  public int getGraphCount() {
    return (graphIds != null) ? graphIds.size() : 0;
  }
}
