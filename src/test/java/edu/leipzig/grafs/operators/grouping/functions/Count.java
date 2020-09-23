package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.operators.grouping.model.AggregationMappingEntry;
import edu.leipzig.grafs.operators.grouping.model.PropertiesAggregationFunction;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class Count extends AggregationMappingEntry {


  public Count(String propertyKey) {
    super(propertyKey, new PropertiesAggregationFunction(PropertyValue.create(0L),
        (PropertyValue pV1, PropertyValue pV2) -> {
          var newVal = new PropertyValue();
          newVal.setLong(pV1.getLong() + 1L);
          return newVal;
        }));
  }
}
