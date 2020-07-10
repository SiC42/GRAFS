package edu.leipzig.grafs.operators.grouping.model;

import java.io.IOException;
import java.io.ObjectStreamException;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class PropertiesAggregationFunction implements
    SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue> {

  private PropertyValue identity;
  private SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue> function;

  public PropertiesAggregationFunction(PropertyValue identity,
      SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue> function) {
    this.identity = identity;
    this.function = function;
  }

  public PropertyValue getIdentity() {
    return identity;
  }

  public PropertyValue apply(PropertyValue pV1, PropertyValue pV2) {
    return function.apply(pV1, pV2);
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws IOException {
    out.writeObject(identity);
    out.writeObject(function);
  }

  private void readObject(java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    this.identity = (PropertyValue) in.readObject();
    this.function = (SerializableBiFunction<PropertyValue, PropertyValue, PropertyValue>) in
        .readObject();

  }

  private void readObjectNoData()
      throws ObjectStreamException {

  }

}
