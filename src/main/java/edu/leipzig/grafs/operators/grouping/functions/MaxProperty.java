package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.model.Element;
import java.util.Objects;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;

public class MaxProperty extends BaseAggregateFunction {

  /**
   * Property key whose value should be aggregated.
   */
  private final String propertyKey;

  /**
   * Creates a new instance of a MaxProperty aggregate function.
   *
   * @param propertyKey property key to aggregate
   */
  public MaxProperty(String propertyKey) {
    this(propertyKey, "max_" + propertyKey);
  }

  /**
   * Creates a new instance of a MaxProperty aggregate function.
   *
   * @param propertyKey          property key to aggregate
   * @param aggregatePropertyKey aggregate property key
   */
  public MaxProperty(String propertyKey, String aggregatePropertyKey) {
    super(aggregatePropertyKey);
    Objects.requireNonNull(propertyKey);
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    return PropertyValueUtils.Numeric.max(aggregate, increment);
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return element.getPropertyValue(propertyKey);
  }

}
