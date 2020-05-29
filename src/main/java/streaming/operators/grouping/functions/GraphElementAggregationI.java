package streaming.operators.grouping.functions;

import java.util.Map;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import streaming.model.EdgeContainer;
import streaming.model.Element;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public interface GraphElementAggregationI extends
    WindowFunction<EdgeContainer, EdgeContainer, String, TimeWindow> {


  default Element aggregateElement(AggregationMapping aggregationMapping,
      GroupingInformation elemGroupInfo, Element aggregatedElement,
      Element curElement) {
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
