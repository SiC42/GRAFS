package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.model.Element;
import java.util.Objects;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;

/**
 * Aggregate function that determines the maximal property value for all elements in a group.
 */
public class MaxProperty extends BaseAggregateFunction {

  /**
   * Aggregate property key, on which the aggregate should be stored upon.
   */
  private static final String AGGREGATE_PROPERTY_KEY_PREFIX = "max_";
  /**
   * Property key whose value should be aggregated.
   */
  private final String propertyKey;

  /**
   * Creates a new instance of a the maximum aggregate function. The aggregate key to be used for
   * the storage of the maximum property value is "{@value #AGGREGATE_PROPERTY_KEY_PREFIX}" +
   * whatever is provided as the aggregate property key.
   *
   * @param propertyKey aggregate property key
   */
  public MaxProperty(String propertyKey) {
    this(propertyKey, AGGREGATE_PROPERTY_KEY_PREFIX + propertyKey);
  }

  /**
   * Creates a new instance of a maximum aggregate function.
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
