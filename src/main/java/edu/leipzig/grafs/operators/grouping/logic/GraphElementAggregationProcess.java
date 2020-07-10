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

  /**
   * Sets the properties on which are grouped on. This method only has to be called once for one
   * group.
   *
   * @param groupInfo  information on which 'fields' should be set for the element
   * @param emptyElem  'empty' element on which the 'fields' should be set
   * @param masterElem element which holds the fields used to set the 'empty' element
   * @return the previous 'empty' element, now with the fields set from the 'master' element
   */
  default GraphElement setGroupedProperties(GroupingInformation groupInfo,
      GraphElement emptyElem, GraphElement masterElem) {
    if (groupInfo == null) {
      return emptyElem;
    } else if (groupInfo.shouldUseLabel()) {
      emptyElem.setLabel(masterElem.getLabel());
    }
    for (var key : groupInfo.getKeys()) {
      emptyElem.setProperty(key, masterElem.getPropertyValue(key));
    }
    // TODO: Deal with memberships
    return emptyElem;
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
