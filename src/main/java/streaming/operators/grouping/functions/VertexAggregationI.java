package streaming.operators.grouping.functions;

import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import streaming.model.Vertex;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.AggregationVertex;
import streaming.operators.grouping.model.GroupingInformation;

public interface VertexAggregationI extends GraphElementAggregationI {

  default AggregationVertex aggregateVertex(AggregationMapping aggregationMapping,
      GroupingInformation groupInfo, AggregationVertex aggregatedVertex,
      Vertex curVertex) {
    for (Property property : curVertex.getProperties()) {
      String key = property.getKey();
      if (groupInfo.groupingKeys.contains(key)) {
        aggregatedVertex.setProperty(key, property.getValue());
      } else if (aggregationMapping.containsAggregationForProperty(key)) {
        PropertiesAggregationFunction aF = aggregationMapping.getAggregationForProperty(key);
        PropertyValue prevValue = aggregatedVertex.hasProperty(key)
            ? aggregatedVertex.getPropertyValue(key)
            : aF.getIdentity();
        PropertyValue newValue = aF.apply(prevValue, curVertex.getPropertyValue(key));
        aggregatedVertex.setProperty(key, newValue);
      }
    }
    for (var graphId : curVertex.getGraphIds()) {
      aggregatedVertex.addGraphId(graphId);
    }
    return aggregatedVertex;
  }

}
