package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.matching.logic.MatchingTestBase;
import edu.leipzig.grafs.util.FlinkConfig;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import edu.leipzig.grafs.util.TestUtils;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class IsomorphismTest extends MatchingTestBase {

  private static FlinkConfig config;

  @BeforeAll
  static void initConfig() {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    config = new FlinkConfigBuilder(env)
        .withWaterMarkStrategy(WatermarkStrategy
            .<EdgeContainer>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
  }

  @Test
  void testWithPaperGraph() throws Exception {
    var loader = graphLoader;
    EdgeStream edgeStream = loader.createEdgeStream(config);
    var appendDsGraph = "iso {}["
        // iso-edges
        // from blue
        + "(v1)-[e1]->(v2)"
        + "(v2)-[e2]->(v1)"
        + "(v2)-[e3]->(v3)"
        + "(v2)-[e4]->(v4)"
        + "(v2)-[e5]->(v5)]";
    loader.appendFromString(appendDsGraph);
    var expectedEcs = loader.createEdgeContainersByGraphVariables("iso");
    var resultStream = edgeStream
        .callForStream(new Isomorphism<>(queryPaperGraphGdlStr,
            TumblingEventTimeWindows.of(Time.milliseconds(10))));
    TestUtils.assertThatStreamContains(resultStream, expectedEcs);
  }
  }

}