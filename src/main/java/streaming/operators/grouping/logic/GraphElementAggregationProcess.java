package streaming.operators.grouping.logic;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.gradoop.common.model.impl.properties.PropertyValue;
import streaming.model.EdgeContainer;
import streaming.model.GraphElement;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;
import streaming.operators.grouping.model.PropertiesAggregationFunction;

public interface GraphElementAggregationProcess extends
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

  default streaming.model.GraphElement setGroupedProperties(GroupingInformation groupInfo,
      streaming.model.GraphElement aggregatedElem, streaming.model.GraphElement curElem) {
    if (groupInfo == null) {
      return aggregatedElem;
    } else if (groupInfo.useLabel) {
      aggregatedElem.setLabel(curElem.getLabel());
    }
    for (var key : groupInfo.groupingKeys) {
      aggregatedElem.setProperty(key, curElem.getPropertyValue(key));
    }
    // TODO: Deal with memberships
    return aggregatedElem;
  }

  default GraphElement aggregateGraphElement(AggregationMapping aggregationMapping,
      GraphElement aggregatedElem,
      GraphElement curElem) {
    if (aggregationMapping != null) {
      for (var aggregationEntry : aggregationMapping.entrySet()) {
        var key = aggregationEntry.getPropertyKey();
        PropertiesAggregationFunction aF = aggregationMapping.getAggregationForProperty(key);
        PropertyValue prevValue = aggregatedElem.hasProperty(key)
            ? aggregatedElem.getPropertyValue(key)
            : aF.getIdentity();
        PropertyValue newValue = aF.apply(prevValue, curElem.getPropertyValue(key));
        aggregatedElem.setProperty(key, newValue);
      }
    }
    for (var graphId : curElem.getGraphIds()) {
      aggregatedElem.addGraphId(graphId);
    }
    return aggregatedElem;
  }

}
