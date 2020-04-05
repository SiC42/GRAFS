package streaming.operators;

import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.FlatMapFunction;
import streaming.model.Edge;
import streaming.model.GraphElementInformation;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.ElementGroupingInformation;
import streaming.model.grouping.PropertiesAggregationFunction;

public interface GraphElementAggregationFunctionI extends FlatMapFunction<Set<Edge>, Edge> {


  default void aggregateGei(AggregationMapping aggregationMapping,
      ElementGroupingInformation elemGroupInfo, GraphElementInformation aggregatedGei,
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
