package edu.leipzig.grafs.operators.grouping.functions;

import java.util.Objects;

public abstract class BaseAggregateFunction implements AggregateFunction {

  /**
   * Key of the aggregate property.
   */
  private String aggregatePropertyKey;

  /**
   * Creates a new instance of a base aggregate function.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public BaseAggregateFunction(String aggregatePropertyKey) {
    setAggregatePropertyKey(aggregatePropertyKey);
  }

  @Override
  public String getAggregatePropertyKey() {
    return aggregatePropertyKey;
  }

  /**
   * Sets the property key used to store the aggregate value.
   *
   * @param aggregatePropertyKey aggregate property key
   */
  public void setAggregatePropertyKey(String aggregatePropertyKey) {
    Objects.requireNonNull(aggregatePropertyKey);
    this.aggregatePropertyKey = aggregatePropertyKey;
  }

  /**
   * Returns {@link PropertyValue#NULL_VALUE}.
   * <p>
   * Overriding this method allows extending this class using lambdas.
   *
   * @return the {@link PropertyValue#NULL_VALUE}
   */
  @Override
  public PropertyValue getDefaultValue() {
    return PropertyValue.NULL_VALUE;
  }

}
