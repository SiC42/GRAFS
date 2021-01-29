package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.model.window.TumblingEventTimeWindows;
import edu.leipzig.grafs.operators.matching.logic.MatchingTestBase;
import edu.leipzig.grafs.util.FlinkConfig;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import edu.leipzig.grafs.util.TestUtils;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
            .<Triplet>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
  }

  @Test
  void testWithPaperGraph() throws Exception {
    var loader = graphLoader;
    var edgeStream = loader.createEdgeStream(config);
    var appendDsGraph = "iso {}["
        // iso-edges
        // from blue
        + "(v1)-[e1]->(v2)"
        + "(v2)-[e2]->(v1)"
        + "(v2)-[e3]->(v3)"
        + "(v2)-[e4]->(v4)"
        + "(v2)-[e5]->(v5)]";
    loader.appendFromString(appendDsGraph);
    var expectedTriplets = loader.createTripletsByGraphVariables("iso");
    var resultStream = edgeStream
        .callForGC(new Isomorphism(queryPaperGraphGdlStr))
        .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .apply();

    TestUtils.assertThatStreamContains(resultStream, expectedTriplets);
  }

  @Test
  void testWithSocialGraph_personKnowsPerson() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(:Person)-[:knows]->(:Person)";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendIsoString = "iso {}["
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
    loader.appendFromString(appendIsoString);
    var expectedTriplets = loader.createTripletsByGraphVariables("iso");

    var resultStream = graphStream
        .callForGC(new Isomorphism(queryStr))
        .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .apply();

    TestUtils.assertThatStreamContains(resultStream, expectedTriplets);
  }

  @Test
  void testWithSocialGraph_personKnowsPersonSince2014() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(:Person)-[:knows {since: 2014}]->(:Person)";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendIsoString = "iso {}["
        + "(alice)-[akb]->(bob)"
        + "(bob)-[bka]->(alice)"
        + "(carol)-[ckd]->(dave)"
        + "(dave)-[dkc]->(carol)"
        + "]";
    loader.appendFromString(appendIsoString);
    var expectedTriplets = loader.createTripletsByGraphVariables("iso");

    var resultStream = graphStream
        .callForGC(new Isomorphism(queryStr))
        .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .apply();

    TestUtils.assertThatStreamContains(resultStream, expectedTriplets);
  }

  @Test
  void testWithSocialGraph_TripleWithPersonVertices() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(:Person)-[]->(:Person)";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendIsoString = "iso {}["
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
    loader.appendFromString(appendIsoString);
    var expectedTriplets = loader.createTripletsByGraphVariables("iso");

    var resultStream = graphStream
        .callForGC(new Isomorphism(queryStr))
        .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .apply();

    TestUtils.assertThatStreamContains(resultStream, expectedTriplets);
  }

  @Test
  void testWithSocialGraph_TripleWithKnowsEdge() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "()-[:knows]->()";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendIsoString = "iso {}["
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
    loader.appendFromString(appendIsoString);
    var expectedTriplets = loader.createTripletsByGraphVariables("iso");

    var resultStream = graphStream
        .callForGC(new Isomorphism(queryStr))
        .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .apply();

    TestUtils.assertThatStreamContains(resultStream, expectedTriplets);
  }

  @Test
  void testWithSocialGraph_PersonsKnowEachOther() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(a)-[:knows]->(b)-[:knows]->(a)";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendIsoString = "iso {}["
        + "(alice)-[akb]->(bob)"
        + "(bob)-[bka]->(alice)"
        + "(bob)-[bkc]->(carol)"
        + "(carol)-[ckb]->(bob)"
        + "(carol)-[ckd]->(dave)"
        + "(dave)-[dkc]->(carol)"
        + "]";
    loader.appendFromString(appendIsoString);
    var expectedTriplets = loader.createTripletsByGraphVariables("iso");

    var resultStream = graphStream
        .callForGC(new Isomorphism(queryStr))
        .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .apply();

    TestUtils.assertThatStreamContains(resultStream, expectedTriplets);
  }

  @Test
  void testWithSocialGraph_forumHasModerator() throws Exception {
    var loader = TestUtils.getSocialNetworkLoader();
    var queryStr = "(:Forum)-[:hasModerator]->()";
    GraphStream graphStream = loader.createEdgeStream(config);
    var appendIsoString = "iso {}["
        + "(gps)-[gpshmod]->(dave)"
        + "(gdbs)-[gdbshmoa]->(alice)"
        + "]";
    loader.appendFromString(appendIsoString);
    var expectedTriplets = loader.createTripletsByGraphVariables("iso");

    var resultStream = graphStream
        .callForGC(new Isomorphism(queryStr))
        .withWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        .apply();

    TestUtils.assertThatStreamContains(resultStream, expectedTriplets);
  }

}