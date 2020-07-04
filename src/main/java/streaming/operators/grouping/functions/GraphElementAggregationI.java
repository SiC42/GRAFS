package streaming.operators.grouping.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import streaming.model.EdgeContainer;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;

public interface GraphElementAggregationI extends
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

}
