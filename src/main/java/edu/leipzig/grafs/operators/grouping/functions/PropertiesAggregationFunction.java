package edu.leipzig.grafs.operators.grouping.functions;

import java.io.IOException;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class PropertiesAggregationFunction implements
    SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue> {

  private String aggregatePropertyKey;
  private PropertyValue identity;
  private SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue> function;
  private boolean isBiFunction;

  public PropertiesAggregationFunction(String aggregatePropertyKey, PropertyValue identity,
      SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue> function) {
    this(aggregatePropertyKey, identity, function, true);
  }

  public PropertiesAggregationFunction(String aggregatePropertyKey, PropertyValue identity,
      SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue> function,
      boolean isBiFunction) {
    this.aggregatePropertyKey = aggregatePropertyKey;
    this.identity = identity;
    this.function = function;
    this.isBiFunction = isBiFunction;
  }

  public String getAggregatePropertyKey() {
    return aggregatePropertyKey;
  }

  public PropertyValue getIdentity() {
    return identity;
  }

  public boolean isBiFunction() {
    return isBiFunction;
  }

  public PropertyValue apply(PropertyValue pV1, PropertyValue pV2) {
    return function.apply(pV1, pV2);
  }

  public PropertyValue apply(PropertyValue pV1) {
    return apply(pV1, null);
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeObject(aggregatePropertyKey);
    out.writeObject(identity);
    out.writeObject(function);
    out.writeObject(isBiFunction);
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    this.aggregatePropertyKey = (String) in.readObject();
    this.identity = (PropertyValue) in.readObject();
    this.function = (SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue>) in
        .readObject();
    this.isBiFunction = (boolean) in
        .readObject();

  }
}
