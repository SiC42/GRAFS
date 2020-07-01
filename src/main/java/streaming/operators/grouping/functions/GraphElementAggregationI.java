package streaming.operators.grouping.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import streaming.model.EdgeContainer;
import streaming.model.GraphElement;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public interface GraphElementAggregationI extends
    WindowFunction<EdgeContainer, EdgeContainer, String, TimeWindow> {


  default void checkAggregationAndGroupingKeyIntersection(AggregationMapping aggregationMapping,
      GroupingInformation elemGroupInfo) {
    for (String key : elemGroupInfo.groupingKeys) {
      if (aggregationMapping.containsAggregationForProperty(key)) {
        throw new RuntimeException(
            String.format("Aggregation key '%s' is also present in grouping keys", key));
      }
    }
    if (elemGroupInfo.useMembership && aggregationMapping.getAggregationForMembership()
        .isPresent()) {
      throw new RuntimeException(
          "Elements should be grouped by membership but aggregation mapping contains function for membership aggregation.");
    }
  }

  default GraphElement aggregateGraphElement(AggregationMapping aggregationMapping,
      GroupingInformation groupInfo, GraphElement aggregatedElement,
      GraphElement curElement) {
    for (Property property : curElement.getProperties()) {
      String key = property.getKey();
      if (groupInfo.groupingKeys.contains(key)) {
        aggregatedElement.setProperty(key, property.getValue());
      } else if (aggregationMapping.containsAggregationForProperty(key)) {
        PropertiesAggregationFunction aF = aggregationMapping.getAggregationForProperty(key);
        PropertyValue prevValue = aggregatedElement.hasProperty(key)
            ? aggregatedElement.getPropertyValue(key)
            : aF.getIdentity();
        PropertyValue newValue = aF.apply(prevValue, curElement.getPropertyValue(key));
        aggregatedElement.setProperty(key, newValue);
      }
    }
    for (var graphId : curElement.getGraphIds()) {
      aggregatedElement.addGraphId(graphId);
    }
    return aggregatedElement;
  }

}
