package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.model.Element;
import java.util.Objects;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Aggregate function that sums the property value (for the given key) for all elements in a group.
 */
public class SumProperty extends
    BaseAggregateFunction implements Sum {

  /**
   * Aggregate property key, on which the aggregate should be stored upon.
   */
  private static final String AGGREGATE_PROPERTY_KEY_PREFIX = "sum_";
  /**
   * Property key whose value should be aggregated.
   */
  private final String propertyKey;

  /**
   * Creates a new instance of a the sum property function. The key to be used for the summation is
   * "{@value #AGGREGATE_PROPERTY_KEY_PREFIX}" + whatever is provided as the aggregate property
   * key.
   *
   * @param propertyKey aggregate property key
   */
  public SumProperty(String propertyKey) {
    this(propertyKey, AGGREGATE_PROPERTY_KEY_PREFIX + propertyKey);
  }

  /**
   * Creates a new instance of a the sum property function.
   *
   * @param propertyKey          aggregate property key
   * @param aggregatePropertyKey specifies where the aggregate should be stored
   */
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
