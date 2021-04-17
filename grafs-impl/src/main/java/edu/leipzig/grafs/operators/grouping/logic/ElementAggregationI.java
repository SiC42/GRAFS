package edu.leipzig.grafs.operators.grouping.logic;

import edu.leipzig.grafs.exceptions.KeyOverlapException;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import java.util.Set;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Interface that provides basic functionalities for aggregating on elements in an edge stream.
 */
public interface ElementAggregationI {

  /**
   * Checks if there is an overlap between the used property keys of the aggregate functions and the
   * grouping keys.
   * <p>
   * It is pointless to aggregate on properties, which are also used for grouping purposes, as the
   * property value of all elements in the group will be the same. Therefore an exception is thrown
   *
   * @param aggregateFunctions aggregate functions to be used in the aggregation process
   * @param elemGroupInfo      grouping information used to determine which properties are used for
   *                           grouping
   * @throws KeyOverlapException if there is an overlap between the used property keys of the
   *                             aggregate functions and the grouping keys.
   */
  default void checkAggregationAndGroupingKeyIntersection(
      Set<AggregateFunction> aggregateFunctions,
      GroupingInformation elemGroupInfo) throws KeyOverlapException {
    for (var function : aggregateFunctions) {
      var key = function.getAggregatePropertyKey();
      if (elemGroupInfo.getKeys().contains(key)) {
        throw new KeyOverlapException(
            String.format("Aggregation key '%s' is also present in grouping keys", key));
      }
    }
  }

  /**
   * Sets the properties on which are grouped on. This method only has to be called once for one
   * group.
   *
   * @param groupInfo   information on which fields are grouped upon
   * @param baseElement element on which the grouped-on fields should be set
   * @param masterElem  element which holds the fields used to set the 'base' element
   * @return the given 'base' element, now with the fields set from the 'master' element
   */
  default <E extends Element> E setGroupedProperties(GroupingInformation groupInfo,
      E baseElement, E masterElem) {
    if (groupInfo != null) {
      if (groupInfo.shouldUseLabel()) {
        baseElement.setLabel(masterElem.getLabel());
      }
      for (var key : groupInfo.getKeys()) {
        if (masterElem.hasProperty(key)) {
          baseElement.setProperty(key, masterElem.getPropertyValue(key));
        } else {
          baseElement.setProperty(key, PropertyValue.NULL_VALUE);
        }
      }
    } else { // null means we group over everything, so copy all information out of master
      baseElement.setLabel(masterElem.getLabel());
      if (masterElem.getProperties() != null) {
        for (var prop : masterElem.getProperties()) {
          baseElement.setProperty(prop);
        }
      }
    }
    return baseElement;
  }

  /**
   * Uses the given aggregate functions to aggregate the value in the given elements and returns the
   * aggregation element with the new aggregates set
   * <p>
   * If the property value of the element is <tt>null</tt>, the value in the aggregation element is
   * not set anew.
   *
   * @param aggregationElement element on which the aggregates should be set
   * @param element            element used to determine the new aggregate
   * @param aggregateFunctions aggregate functions used for the aggregation
   * @return the aggregation element with the new aggregates
   */
  default <E extends Element> E aggregateElement(
      E aggregationElement, E element,
      Set<AggregateFunction> aggregateFunctions) {
    for (AggregateFunction aggFunc : aggregateFunctions) {
      PropertyValue increment = aggFunc.getIncrement(element);
      String key = aggFunc.getAggregatePropertyKey();
      if (increment != null) {
        PropertyValue aggregated;
        if (aggregationElement.hasProperty(key)) {
          PropertyValue aggregate = aggregationElement.getPropertyValue(key);
          aggregated = aggFunc.aggregate(aggregate, increment);
        } else {
          aggregated = increment.copy();
        }
        aggregationElement.setProperty(key, aggregated);
      }
    }
    return aggregationElement;
  }

  /**
   * Used in the end of the aggregation process. Determines if there are aggregations missing in the
   * aggregation element (i.e. no element had the property set during the aggregation process). If
   * this is the case the default value ({@link AggregateFunction#getDefaultValue()} will be set.
   *
   * @param aggregateFunctions aggregate functions that were used for the aggregation process
   * @param aggregationElement element on which the aggregate was applied upon
   * @return the aggregation element with all missing aggregate set to the default value
   */
  default <E extends Element> E checkForMissingAggregationsAndApply(
      Set<AggregateFunction> aggregateFunctions,
      E aggregationElement) {
    for (var func : aggregateFunctions) {
      var aggregateKey = func.getAggregatePropertyKey();
      if (!aggregationElement.hasProperty(aggregateKey)) {
        aggregationElement.setProperty(func.getAggregatePropertyKey(), func.getDefaultValue());
      }
    }
    return aggregationElement;

  }

}
