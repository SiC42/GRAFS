package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.model.Element;
import java.util.Objects;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;

/**
 * Aggregate function that determines the minimal property value for all elements in a group.
 */
public class MinProperty extends BaseAggregateFunction {

  /**
   * Aggregate property key, on which the aggregate should be stored upon.
   */
  private static final String AGGREGATE_PROPERTY_KEY_PREFIX = "min_";

  /**
   * Property key whose value should be aggregated.
   */
  private final String propertyKey;

  /**
   * Creates a new instance of a the minimum aggregate function. The aggregate key to be used for
   * the storage of the minimal property value is "{@value #AGGREGATE_PROPERTY_KEY_PREFIX}" +
   * whatever is provided as the aggregate property key.
   *
   * @param propertyKey aggregate property key
   */
  public MinProperty(String propertyKey) {
    this(propertyKey, AGGREGATE_PROPERTY_KEY_PREFIX + propertyKey);
  }

  /**
   * Creates a new instance of a minimum aggregate function.
   *
   * @param propertyKey          property key to aggregate
   * @param aggregatePropertyKey aggregate property key
   */
  public MinProperty(String propertyKey, String aggregatePropertyKey) {
    super(aggregatePropertyKey);
    Objects.requireNonNull(propertyKey);
    this.propertyKey = propertyKey;
  }

  @Override
  public PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment) {
    return PropertyValueUtils.Numeric.min(aggregate, increment);
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return element.getPropertyValue(propertyKey);
  }

}
