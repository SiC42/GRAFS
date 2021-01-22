package edu.leipzig.grafs.benchmark;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import edu.leipzig.grafs.benchmark.operators.matching.BenchmarkDualSimulation;
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

public class BenchmarkDualSimulationTest extends MatchingTestBase {


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
    var appendDsGraph = "ds {}["
        // ds-edges
        // from blue
        + "(v1)-[e1]->(v2)"
        + "(v2)-[e2]->(v1)"
        + "(v2)-[e3]->(v3)"
        + "(v2)-[e4]->(v4)"
        + "(v2)-[e5]->(v5)"
        + "(v2)-[e6]->(v6)"
        // from green
        + "(v6)-[e7]->(v7)"
        + "(v7)-[e9]->(v8)"
        + "(v7)-[e10]->(v5)"
        + "(v7)-[e11]->(v9)"
        + "(v8)-[e13]->(v2)"
        // from pink
        + "(v12)-[e15]->(v13)"
        + "(v13)-[e16]->(v12)"
        + "(v13)-[e17]->(v14)"
        // from yellow
        + "(v15)-[e18]->(v14)"
        + "(v15)-[e19]->(v16)"
        + "(v16)-[e20]->(v17)"
        + "(v17)-[e21]->(v18)"
        + "(v17)-[e22]->(v19)"
        + "(v19)-[e25]->(v21)"
        + "(v20)-[e26]->(v15)"
        + "(v21)-[e27]->(v20)"
        + "(v21)-[e28]->(v22)"
        // from grey
        + "(v23)-[e29]->(v22)"
        + "(v23)-[e30]->(v24)"
        + "(v24)-[e31]->(v26)"
        + "(v24)-[e31]->(v27)"
        + "(v26)-[e32]->(v25)"
        + "(v26)-[e33]->(v27)"
        + "(v27)-[e34]->(v29)"
        + "(v29)-[e35]->(v28)"
        + "(v29)-[e36]->(v30)"
        + "(v30)-[e37]->(v23)]";
    graphLoader.appendFromString(appendDsGraph);
    var expectedEcs = graphLoader.createTripletsByGraphVariables("ds");

    Iterator<Triplet> matchedEcIt = edgeStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new BenchmarkDualSimulation(queryPaperGraphGdlStr))
        .collect();
    var actualEcs = new ArrayList<Triplet>();
    matchedEcIt.forEachRemaining(actualEcs::add);
    assertThat(actualEcs, containsInAnyOrder(expectedEcs.toArray()));
  }

}