package streaming.operators.grouping.functions;

import java.util.Map;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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
      GroupingInformation elemGroupInfo, GraphElement aggregatedElement,
      GraphElement curElement) {
    for (Map.Entry<String, String> property : curElement.getProperties().entrySet()) {
      String key = property.getKey();
      if (elemGroupInfo.groupingKeys.contains(key)) {
        aggregatedElement.addProperty(key, property.getValue());
      } else if (aggregationMapping.containsAggregationForProperty(key)) {
        PropertiesAggregationFunction aF = aggregationMapping.getAggregationForProperty(key);
        String prevValue = aggregatedElement.containsProperty(key)
            ? aggregatedElement.getProperty(key)
            : aF.getIdentity();
        String newValue = aF.apply(prevValue, curElement.getProperty(key));
        aggregatedElement.addProperty(key, newValue);
      }
    }
    return aggregatedElement;
  }

}
