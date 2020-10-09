package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.model.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Aggregate function that simply counts the number of elements in a group.
 */
public class Count extends BaseAggregateFunction implements Sum {

  /**
   * Creates a new instance of a the count function.
   *
   * @param propertyKey aggregate property key
   */
  public Count(String propertyKey) {
    super(propertyKey);
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return PropertyValue.create(1L);
  }

  @Override
  public PropertyValue getDefaultValue() {
    return PropertyValue.create(0L);
  }
}
