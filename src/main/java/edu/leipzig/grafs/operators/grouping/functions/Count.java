package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.model.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class Count extends BaseAggregateFunction implements Sum {


  public Count(String propertyKey) {
    super(propertyKey);
  }

  @Override
  public PropertyValue getIncrement(Element element) {
    return PropertyValue.create(1L);
  }

  @Override
  public PropertyValue getDefaultValue() {
    return PropertyValue.create(0L);
  }
}
