package streaming.operators.grouping.functions;

import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import streaming.model.Edge;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public interface EdgeAggregationI extends VertexAggregationI {

  default Edge aggregateEdge(AggregationMapping aggregationMapping,
      GroupingInformation groupInfo, Edge aggregatedEdge,
      Edge curEdge) {
    for (Property property : curEdge.getProperties()) {
      String key = property.getKey();
      if (groupInfo.groupingKeys.contains(key)) {
        aggregatedEdge.setProperty(key, property.getValue());
      } else if (aggregationMapping.containsAggregationForProperty(key)) {
        PropertiesAggregationFunction aF = aggregationMapping.getAggregationForProperty(key);
        PropertyValue prevValue = aggregatedEdge.hasProperty(key)
            ? aggregatedEdge.getPropertyValue(key)
            : aF.getIdentity();
        PropertyValue newValue = aF.apply(prevValue, curEdge.getPropertyValue(key));
        aggregatedEdge.setProperty(key, newValue);
      }
    }
    for (var graphId : curEdge.getGraphIds()) {
      aggregatedEdge.addGraphId(graphId);
    }
    return aggregatedEdge;
  }

}
