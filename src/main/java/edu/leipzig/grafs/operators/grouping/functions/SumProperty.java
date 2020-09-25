package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.model.Element;
import java.util.Objects;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class SumProperty extends
    BaseAggregateFunction implements Sum {

  /**
   * Property key whose value should be aggregated.
   */
  private final String propertyKey;

  public SumProperty(String propertyKey) {
    this(propertyKey, "sum_" + propertyKey);
  }

  public SumProperty(String propertyKey, String aggregatePropertyKey) {
    super(aggregatePropertyKey);
    Objects.requireNonNull(propertyKey);
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return element.getPropertyValue(propertyKey);
  }

}
