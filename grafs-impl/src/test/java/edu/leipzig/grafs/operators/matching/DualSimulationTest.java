package edu.leipzig.grafs.operators.matching;


import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.GraphStream;
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

public class DualSimulationTest extends MatchingTestBase {


  private static FlinkConfig config;

  @BeforeAll
  static void initConfig() {
    StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    config = new FlinkConfigBuilder(env)
        .withWaterMarkStrategy(WatermarkStrategy
            .<Triplet>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
  }

  @Test
  void testPaperGraph() throws Exception {
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
    var loader = graphLoader;
    GraphStream graphStream = loader.createEdgeStream(config);
    loader.appendFromString(appendDsGraph);
    var expectedEcs = loader.createTripletsByGraphVariables("ds");

    var resultStream = graphStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new DualSimulation(queryPaperGraphGdlStr));
    TestUtils.assertThatStreamContains(resultStream, expectedEcs);
  }

  @Test
  void testWithSocialGraph_personKnowsPerson() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(:Person)-[:knows]->(:Person)";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendDsString = "ds {}["
        + "(alice)-[akb]->(bob)"
        + "(bob)-[bka]->(alice)"
        + "(bob)-[bkc]->(carol)"
        + "(carol)-[ckb]->(bob)"
        + "(carol)-[ckd]->(dave)"
        + "(dave)-[dkc]->(carol)"
        + "(eve)-[eka]->(alice)"
        + "(eve)-[ekb]->(bob)"
        + "(frank)-[fkc]->(carol)"
        + "(frank)-[fkd]->(dave)"
        + "]";
    loader.appendFromString(appendDsString);
    var expectedEcs = loader.createTripletsByGraphVariables("ds");

    var resultStream = graphStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new DualSimulation(queryStr));

    TestUtils.assertThatStreamContains(resultStream, expectedEcs);
  }

  @Test
  void testWithSocialGraph_personKnowsPersonSince2014() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(:Person)-[:knows {since: 2014}]->(:Person)";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendDsString = "ds {}["
        + "(alice)-[akb]->(bob)"
        + "(bob)-[bka]->(alice)"
        + "(carol)-[ckd]->(dave)"
        + "(dave)-[dkc]->(carol)"
        + "]";
    loader.appendFromString(appendDsString);
    var expectedEcs = loader.createTripletsByGraphVariables("ds");

    var resultStream = graphStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new DualSimulation(queryStr));

    TestUtils.assertThatStreamContains(resultStream, expectedEcs);
  }

  @Test
  void testWithSocialGraph_TripleWithPersonVertices() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(:Person)-[]->(:Person)";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendDsString = "ds {}["
        + "(alice)-[akb]->(bob)"
        + "(bob)-[bka]->(alice)"
        + "(bob)-[bkc]->(carol)"
        + "(carol)-[ckb]->(bob)"
        + "(carol)-[ckd]->(dave)"
        + "(dave)-[dkc]->(carol)"
        + "(eve)-[eka]->(alice)"
        + "(eve)-[ekb]->(bob)"
        + "(frank)-[fkc]->(carol)"
        + "(frank)-[fkd]->(dave)"
        + "]";
    loader.appendFromString(appendDsString);
    var expectedEcs = loader.createTripletsByGraphVariables("ds");

    var resultStream = graphStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new DualSimulation(queryStr));

    TestUtils.assertThatStreamContains(resultStream, expectedEcs);
  }

  @Test
  void testWithSocialGraph_TripleWithKnowsEdge() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "()-[:knows]->()";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendDsString = "ds {}["
        + "(alice)-[akb]->(bob)"
        + "(bob)-[bka]->(alice)"
        + "(bob)-[bkc]->(carol)"
        + "(carol)-[ckb]->(bob)"
        + "(carol)-[ckd]->(dave)"
        + "(dave)-[dkc]->(carol)"
        + "(eve)-[eka]->(alice)"
        + "(eve)-[ekb]->(bob)"
        + "(frank)-[fkc]->(carol)"
        + "(frank)-[fkd]->(dave)"
        + "]";
    loader.appendFromString(appendDsString);
    var expectedEcs = loader.createTripletsByGraphVariables("ds");

    var resultStream = graphStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new DualSimulation(queryStr));

    TestUtils.assertThatStreamContains(resultStream, expectedEcs);
  }

  @Test
  void testWithSocialGraph_PersonsKnowEachOther() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(a)-[:knows]->(b)-[:knows]->(a)";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendDsString = "ds {}["
        + "(alice)-[akb]->(bob)"
        + "(bob)-[bka]->(alice)"
        + "(bob)-[bkc]->(carol)"
        + "(carol)-[ckb]->(bob)"
        + "(carol)-[ckd]->(dave)"
        + "(dave)-[dkc]->(carol)"
        + "]";
    loader.appendFromString(appendDsString);
    var expectedEcs = loader.createTripletsByGraphVariables("ds");

    var resultStream = graphStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new DualSimulation(queryStr));

    TestUtils.assertThatStreamContains(resultStream, expectedEcs);
  }

  @Test
  void testWithSocialGraph_forumHasModerator() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(:Forum)-[:hasModerator]->()";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendDsString = "ds {}["
        + "(gps)-[gpshmod]->(dave)"
        + "(gdbs)-[gdbshmoa]->(alice)"
        + "]";
    loader.appendFromString(appendDsString);
    var expectedEcs = loader.createTripletsByGraphVariables("ds");

    var resultStream = graphStream
        .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .callForGC(new DualSimulation(queryStr));

    TestUtils.assertThatStreamContains(resultStream, expectedEcs);
  }
}