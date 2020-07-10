package edu.leipzig.grafs.operators.grouping.model;

public class AggregationMappingEntry {

  private final String propertyKey;
  private final PropertiesAggregationFunction function;

  public AggregationMappingEntry(final String propertyKey,
      final PropertiesAggregationFunction function) {
    this.propertyKey = propertyKey;
    this.function = function;
  }

  public String getPropertyKey() {
    return propertyKey;
  }

  public PropertiesAggregationFunction getAggregationFunction() {
    return function;
  }

}
