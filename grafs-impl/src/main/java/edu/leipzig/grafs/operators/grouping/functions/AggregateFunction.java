package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.model.Element;
import java.io.Serializable;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Interface for all aggregate functions used in the process of grouping.
 */
public interface AggregateFunction extends Serializable {

  /**
   * Describes the aggregation logic.
   *
   * @param aggregate previously aggregated value
   * @param increment value that is added to the aggregate
   * @return new aggregate
   */
  PropertyValue aggregate(PropertyValue aggregate, PropertyValue increment);

  /**
   * Returns the property key used to store the aggregate value.
   *
   * @return aggregate property key
   */
  String getAggregatePropertyKey();

  /**
   * Describes the increment of an element that should be added to the aggregate.
   *
   * @param element element used to get the increment
   * @return increment, may be {@link PropertyValue#NULL_VALUE}, which is handled by the operator
   */
  PropertyValue getIncrement(Element element);

  /**
   * Returns {@link PropertyValue#NULL_VALUE}.
   *
   * @return the {@link PropertyValue#NULL_VALUE}
   */
  default PropertyValue getDefaultValue() {
    return PropertyValue.NULL_VALUE;
  }
}
