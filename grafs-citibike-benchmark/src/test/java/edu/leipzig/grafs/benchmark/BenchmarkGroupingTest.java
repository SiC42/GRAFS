package edu.leipzig.grafs.benchmark;

import static edu.leipzig.grafs.util.TestUtils.getSocialNetworkLoader;
import static edu.leipzig.grafs.util.TestUtils.validateEdgeContainerCollections;

import edu.leipzig.grafs.benchmark.operators.grouping.BenchmarkGrouping;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.functions.Count;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.FlinkConfig;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Set;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BenchmarkGroupingTest {

  private static FlinkConfig config;

  @BeforeAll
  static void initConfig() {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    config = new FlinkConfigBuilder(env)
        .withWaterMarkStrategy(WatermarkStrategy
            .<EdgeContainer>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
  }

  @BeforeEach
  void init() {
    var env = config.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    FlinkConfig config = new FlinkConfigBuilder(env)
        .withWaterMarkStrategy(WatermarkStrategy
            .<EdgeContainer>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
  }

  @Test
  public void testGroupingOnSocialNetwork() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    var finalStream = edgeStream
        .callForStream(
            new BenchmarkGrouping<>(
                Set.of(GroupingInformation.LABEL_SYMBOL, "city"),
                Set.of(new Count("count")),
                Set.of(GroupingInformation.LABEL_SYMBOL, "since"),
                Set.of(new Count("count")),
                TumblingEventTimeWindows.of(Time.milliseconds(10)),
                null
            )
        );
    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }
    loader.appendFromString("expected[" +
        "(pL:Person {city : \"Leipzig\", count : 2L})" +
        "(pD:Person {city : \"Dresden\", count : 3L})" +
        "(pB:Person {city : \"Berlin\", count : 1L})" +
        "(pD)-[:knows {since : 2014, count : 2L}]->(pD)" +
        "(pD)-[:knows {since : 2013, count : 2L}]->(pL)" +
        "(pD)-[:knows {since : 2015, count : 1L}]->(pL)" +
        "(pL)-[:knows {since : 2014, count : 2L}]->(pL)" +
        "(pL)-[:knows {since : 2013, count : 1L}]->(pD)" +
        "(pB)-[:knows {since : 2015, count : 2L}]->(pD)" +
        "]");
    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

}