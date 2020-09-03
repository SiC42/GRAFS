package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.GraphElement;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.gradoop.common.model.impl.properties.PropertyValue;

public abstract class GraphElementAggregationProcess<W extends Window> extends
    ProcessWindowFunction<EdgeContainer, EdgeContainer, String, W> {

  protected void checkAggregationAndGroupingKeyIntersection(AggregationMapping aggregationMapping,
      GroupingInformation elemGroupInfo) {
    for (var key : elemGroupInfo.getKeys()) {
      if (aggregationMapping.containsAggregationForProperty(key)) {
        throw new RuntimeException(
            String.format("Aggregation key '%s' is also present in grouping keys", key));
      }
    }
  }

  /**
   * Sets the properties on which are grouped on. This method only has to be called once for one
   * group.
   *
   * @param groupInfo  information on which fields are grouped upon
   * @param emptyElem  element on which the grouped-on fields should be set
   * @param masterElem element which holds the fields used to set the 'empty' element
   * @return the given 'empty' element, now with the fields set from the 'master' element
   */
  protected GraphElement setGroupedProperties(GroupingInformation groupInfo,
      GraphElement emptyElem, GraphElement masterElem) {
    if (groupInfo != null) {
      if (groupInfo.shouldUseLabel()) {
        emptyElem.setLabel(masterElem.getLabel());
      }
      for (var key : groupInfo.getKeys()) {
        emptyElem.setProperty(key, masterElem.getPropertyValue(key));
      }
    } else { // null means we group over everything, so copy all information out of master
      emptyElem.setLabel(masterElem.getLabel());
      for (var prop : masterElem.getProperties()) {
        emptyElem.setProperty(prop);
      }
    }
    return emptyElem;
  }

  protected GraphElement aggregateGraphElement(AggregationMapping aggregationMapping,
      GraphElement aggregatedElem,
      GraphElement curElem) {
    if (aggregationMapping != null) {
      for (var aggregationEntry : aggregationMapping.entrySet()) {
        var key = aggregationEntry.getPropertyKey();
        var aggFunc = aggregationMapping.getAggregationForProperty(key);
        PropertyValue prevValue = aggregatedElem.hasProperty(key)
            ? aggregatedElem.getPropertyValue(key)
            : aggFunc.getIdentity();
        PropertyValue curValue = curElem.hasProperty(key)
            ? curElem.getPropertyValue(key)
            : aggFunc.getIdentity();
        PropertyValue newValue = aggFunc.apply(prevValue, curValue);
        aggregatedElem.setProperty(key, newValue);
      }
    }
    // TODO: Deal with memberships
    return aggregatedElem;
  }

}
