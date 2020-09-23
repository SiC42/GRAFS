package edu.leipzig.grafs.operators.grouping.functions;

import edu.leipzig.grafs.operators.grouping.model.AggregationMappingEntry;
import org.gradoop.common.model.impl.properties.PropertyValue;

public class SumProperty extends AggregationMappingEntry {

  private final static int IDENTITY = 0;

  public SumProperty(String propertyKey){
    this(propertyKey, "sum_" + propertyKey);
  }

  public SumProperty(String propertyKey, String aggregatePropertyKey) {
    super(propertyKey,
        new PropertiesAggregationFunction(aggregatePropertyKey, PropertyValue.create(0),
            (PropertyValue pV1, PropertyValue pV2) -> {
              if (!hasBeenSetBefore(pV1) && !pV2.isInt()) {
                return PropertyValue.NULL_VALUE;
              } else if (!hasBeenSetBefore(pV1) && pV2.isInt()) {
                return pV2;
              } else if (hasBeenSetBefore(pV1) && !pV2.isInt()) {
                return pV1;
              } else { //if (isAlreadyBeenSet(pV1) && pV2.isInt())
                var newVal = new PropertyValue();
                newVal.setInt(pV1.getInt() + pV2.getInt());
                return newVal;
              }
            }));
  }

  private static boolean hasBeenSetBefore(PropertyValue pV1) {
    return !pV1.isNull() && pV1.getInt() != IDENTITY;
  }

}
