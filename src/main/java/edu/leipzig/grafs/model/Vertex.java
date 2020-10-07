package edu.leipzig.grafs.model;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Data model that represents the vertex in the property graph model (with graph membership).
 */
public class Vertex extends GraphElement {

  /**
   * Empty constructor mainly for serialization purposes. Creates a new vertex with default
   * parameters, i.e. default label and no properties and graph IDs.
   */
  public Vertex() {
    super();
  }


  /**
   * Creates a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex label
   * @param properties vertex properties
   * @param graphIds   graphs that vertex is contained in
   */
  public Vertex(final GradoopId id, final String label,
      final Properties properties, final GradoopIdSet graphIds) {
    super(id, label, properties, graphIds);
  }


  @Override
  public String toString() {
    return String.format("(%s)", super.toString());
  }
}
