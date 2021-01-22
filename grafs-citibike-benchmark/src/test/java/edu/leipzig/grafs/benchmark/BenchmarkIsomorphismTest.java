package edu.leipzig.grafs.benchmark;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import edu.leipzig.grafs.benchmark.operators.matching.BenchmarkIsomorphism;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.util.FlinkConfig;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BenchmarkIsomorphismTest extends MatchingTestBase {

  private static FlinkConfig config;
  GraphStream edgeStream;

  @BeforeAll
  static void initConfig() {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    config = new FlinkConfigBuilder(env)
        .withWaterMarkStrategy(WatermarkStrategy
            .<Triplet>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
  }

  @BeforeEach
  void init() {
    var env = config.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    FlinkConfig config = new FlinkConfigBuilder(env)
        .withWaterMarkStrategy(WatermarkStrategy
            .<Triplet>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
    edgeStream = graphLoader.createEdgeStream(config);
  }

  @Test
  void test() throws Exception {
    var appendDsGraph = "iso {}["
        // iso-edges
        // from blue
        + "(v1)-[e1]->(v2)"
        + "(v2)-[e2]->(v1)"
        + "(v2)-[e3]->(v3)"
        + "(v2)-[e4]->(v4)"
        + "(v2)-[e5]->(v5)]";
    graphLoader.appendFromString(appendDsGraph);
    var expectedEcs = graphLoader.createTripletsByGraphVariables("iso");

    Iterator<Triplet> matchedEcIt = edgeStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new BenchmarkIsomorphism(queryPaperGraphGdlStr))
        .collect();
    var actualEcs = new ArrayList<Triplet>();
    matchedEcIt.forEachRemaining(actualEcs::add);
    assertThat(actualEcs, containsInAnyOrder(expectedEcs.toArray()));
  }

}