package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.GraphElement;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.grouping.model.PropertiesAggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.gradoop.common.model.impl.properties.PropertyValue;

public interface GraphElementAggregationProcess extends
    WindowFunction<EdgeContainer, EdgeContainer, String, TimeWindow> {

  default void checkAggregationAndGroupingKeyIntersection(AggregationMapping aggregationMapping,
      GroupingInformation elemGroupInfo) {
    for (String key : elemGroupInfo.getKeys()) {
      if (aggregationMapping.containsAggregationForProperty(key)) {
        throw new RuntimeException(
            String.format("Aggregation key '%s' is also present in grouping keys", key));
      }
    }
    if (elemGroupInfo.shouldUseMembership() && aggregationMapping.getAggregationForMembership()
        .isPresent()) {
      throw new RuntimeException(
          "Elements should be grouped by membership but aggregation mapping contains function for membership aggregation.");
    }
  }

  default GraphElement setGroupedProperties(GroupingInformation groupInfo,
      GraphElement aggregatedElem, GraphElement curElem) {
    if (groupInfo == null) {
      return aggregatedElem;
    } else if (groupInfo.shouldUseLabel()) {
      aggregatedElem.setLabel(curElem.getLabel());
    }
    for (var key : groupInfo.getKeys()) {
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
