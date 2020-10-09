package edu.leipzig.grafs.model;


import java.util.Objects;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Data model represents the a graph element of a graph. While not part of the property graph model,
 * this abstraction eases the modeling of other elements, namely {@link Edge} & {@link Vertex}.
 */
public abstract class GraphElement extends Element {

  private GradoopIdSet graphIds;

  /**
   * Default constructor.
   */
  public GraphElement() {
    super();
    graphIds = new GradoopIdSet();
  }

  /**
   * Creates a graph element using the given arguments.
   *
   * @param id         element id
   * @param label      element label
   * @param properties element properties
   * @param graphIds   graphIds that element is contained in
   */
  protected GraphElement(GradoopId id, String label,
      Properties properties, GradoopIdSet graphIds) {
    super(id, label, properties);
    Objects.requireNonNull(graphIds);
    this.graphIds = graphIds;
  }

  /**
   * Returns the graph IDs for this graph element.
   *
   * @return the graph IDs for this graph element
   */
  public GradoopIdSet getGraphIds() {
    return graphIds;
  }

  /**
   * Sets the graph IDs for this graph element to the given ID set.
   *
   * @param graphIds ID set to which this graph element's ID set should be set to
   */
  public void setGraphIds(GradoopIdSet graphIds) {
    this.graphIds = graphIds;
  }

  /**
   * Adds a graph ID to the graph ID set.
   *
   * @param graphId graph ID that should be added to this graph element
   */
  public void addGraphId(GradoopId graphId) {
    graphIds.add(graphId);
  }

  /**
   * Resets the graph ID set of this graph element, i.e. the ID set is empty afterwards.
   */
  public void resetGraphIds() {
    graphIds.clear();
  }

  /**
   * Returns the number of graphs to which this graph element belongs to.
   *
   * @return the number of graphs to which this graph element belongs to
   */
  public int getGraphCount() {
    return (graphIds != null) ? graphIds.size() : 0;
  }

  @Override
  public String toString() {
    return String.format("%s @ %s", super.toString(), graphIds);
  }
}