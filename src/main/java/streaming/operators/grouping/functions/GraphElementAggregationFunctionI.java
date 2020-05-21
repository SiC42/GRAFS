package streaming.operators.grouping.functions;

import java.util.Collection;
import java.util.Map;
import org.apache.flink.api.common.functions.FlatMapFunction;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public interface GraphElementAggregationFunctionI extends FlatMapFunction<Collection<Edge>, Edge> {


  default void aggregateGei(AggregationMapping aggregationMapping,
      GroupingInformation elemGroupInfo, GraphElementInformation aggregatedGei,
      GraphElementInformation graphElementInfo) {
    for (Map.Entry<String, String> property : graphElementInfo.getProperties().entrySet()) {
      String key = property.getKey();
      if (elemGroupInfo.groupingKeys.contains(key)) {
        aggregatedGei.addProperty(key, property.getValue());
      } else if (aggregationMapping.contains(key)) {
        PropertiesAggregationFunction aF = aggregationMapping.get(key);
        String prevValue = aggregatedGei.containsProperty(key)
            ? aggregatedGei.getProperty(key)
            : aF.getIdentity();
        String newValue = aF.apply(prevValue, graphElementInfo.getProperty(key));
        aggregatedGei.addProperty(key, newValue);
      }
    }
  }
}
