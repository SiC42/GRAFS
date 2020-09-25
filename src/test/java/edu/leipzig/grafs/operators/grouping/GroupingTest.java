package edu.leipzig.grafs.operators.grouping;

import static edu.leipzig.grafs.util.TestUtils.getSocialNetworkLoader;
import static edu.leipzig.grafs.util.TestUtils.validateEdgeContainerCollections;
import static org.gradoop.common.util.GradoopConstants.NULL_STRING;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.functions.Count;
import edu.leipzig.grafs.operators.grouping.functions.MaxProperty;
import edu.leipzig.grafs.operators.grouping.functions.MinProperty;
import edu.leipzig.grafs.operators.grouping.functions.SumProperty;
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

public class GroupingTest {

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
            Grouping.createGrouping()
                .addVertexGroupingKeys(Set.of(GroupingInformation.LABEL_SYMBOL, "city"))
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeGroupingKeys(Set.of(GroupingInformation.LABEL_SYMBOL, "since"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
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

  @Test
  public void testVertexPropertySymmetricGraph() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "g2");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    loader.appendFromString("expected[" +
        "(leipzig {city : \"Leipzig\", count : 2L})" +
        "(dresden {city : \"Dresden\", count : 2L})" +
        "(leipzig)-[{count : 2L}]->(leipzig)" +
        "(leipzig)-[{count : 1L}]->(dresden)" +
        "(dresden)-[{count : 2L}]->(dresden)" +
        "(dresden)-[{count : 1L}]->(leipzig)" +
        "]");

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testSingleVertexProperty() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    loader.appendFromString("expected[" +
        "(leipzig {city : \"Leipzig\", count : 2L})" +
        "(dresden {city : \"Dresden\", count : 3L})" +
        "(berlin  {city : \"Berlin\",  count : 1L})" +
        "(dresden)-[{count : 2L}]->(dresden)" +
        "(dresden)-[{count : 3L}]->(leipzig)" +
        "(leipzig)-[{count : 2L}]->(leipzig)" +
        "(leipzig)-[{count : 1L}]->(dresden)" +
        "(berlin)-[{count : 2L}]->(dresden)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMultipleVertexProperties() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    loader.appendFromString("expected[" +
        "(leipzigF {city : \"Leipzig\", gender : \"f\", count : 1L})" +
        "(leipzigM {city : \"Leipzig\", gender : \"m\", count : 1L})" +
        "(dresdenF {city : \"Dresden\", gender : \"f\", count : 2L})" +
        "(dresdenM {city : \"Dresden\", gender : \"m\", count : 1L})" +
        "(berlinM  {city : \"Berlin\", gender : \"m\",  count : 1L})" +
        "(leipzigF)-[{count : 1L}]->(leipzigM)" +
        "(leipzigM)-[{count : 1L}]->(leipzigF)" +
        "(leipzigM)-[{count : 1L}]->(dresdenF)" +
        "(dresdenF)-[{count : 1L}]->(leipzigF)" +
        "(dresdenF)-[{count : 2L}]->(leipzigM)" +
        "(dresdenF)-[{count : 1L}]->(dresdenM)" +
        "(dresdenM)-[{count : 1L}]->(dresdenF)" +
        "(berlinM)-[{count : 1L}]->(dresdenF)" +
        "(berlinM)-[{count : 1L}]->(dresdenM)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .addVertexGroupingKey("gender")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testSingleVertexPropertyWithAbsentValue() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "g3");

    loader.appendFromString("expected[" +
        "(dresden {city : \"Dresden\", count : 2L})" +
        "(others  {city : " + NULL_STRING + ", count : 1L})" +
        "(others)-[{count : 3L}]->(dresden)" +
        "(dresden)-[{count : 1L}]->(dresden)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMultipleVertexPropertiesWithAbsentValue() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "g3");

    loader.appendFromString("expected[" +
        "(dresdenF {city : \"Dresden\", gender : \"f\", count : 1L})" +
        "(dresdenM {city : \"Dresden\", gender : \"m\", count : 1L})" +
        "(others  {city : " + NULL_STRING + ", gender : " + NULL_STRING + ", count : 1L})" +
        "(others)-[{count : 2L}]->(dresdenM)" +
        "(others)-[{count : 1L}]->(dresdenF)" +
        "(dresdenF)-[{count : 1L}]->(dresdenM)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .addVertexGroupingKey("gender")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testSingleVertexAndSingleEdgeProperty() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    loader.appendFromString("expected[" +
        "(leipzig {city : \"Leipzig\", count : 2L})" +
        "(dresden {city : \"Dresden\", count : 3L})" +
        "(berlin  {city : \"Berlin\",  count : 1L})" +
        "(dresden)-[{since : 2014, count : 2L}]->(dresden)" +
        "(dresden)-[{since : 2013, count : 2L}]->(leipzig)" +
        "(dresden)-[{since : 2015, count : 1L}]->(leipzig)" +
        "(leipzig)-[{since : 2014, count : 2L}]->(leipzig)" +
        "(leipzig)-[{since : 2013, count : 1L}]->(dresden)" +
        "(berlin)-[{since : 2015, count : 2L}]->(dresden)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .addEdgeGroupingKey("since")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10))));

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testSingleVertexPropertyAndMultipleEdgeProperties() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("" +
        "input[" +
        "(v0 {a : 0,b : 0})" +
        "(v1 {a : 0,b : 1})" +
        "(v2 {a : 0,b : 1})" +
        "(v3 {a : 1,b : 0})" +
        "(v4 {a : 1,b : 1})" +
        "(v5 {a : 1,b : 0})" +
        "(v0)-[{a : 0,b : 1}]->(v1)" +
        "(v0)-[{a : 0,b : 2}]->(v2)" +
        "(v1)-[{a : 0,b : 3}]->(v2)" +
        "(v2)-[{a : 0,b : 2}]->(v3)" +
        "(v2)-[{a : 0,b : 1}]->(v3)" +
        "(v4)-[{a : 1,b : 2}]->(v2)" +
        "(v5)-[{a : 1,b : 3}]->(v2)" +
        "(v3)-[{a : 2,b : 3}]->(v4)" +
        "(v4)-[{a : 2,b : 1}]->(v5)" +
        "(v5)-[{a : 2,b : 0}]->(v3)" +
        "]"
    );

    loader.appendFromString("expected[" +
        "(v00 {a : 0,count : 3L})" +
        "(v01 {a : 1,count : 3L})" +
        "(v00)-[{a : 0,b : 1,count : 1L}]->(v00)" +
        "(v00)-[{a : 0,b : 2,count : 1L}]->(v00)" +
        "(v00)-[{a : 0,b : 3,count : 1L}]->(v00)" +
        "(v01)-[{a : 2,b : 0,count : 1L}]->(v01)" +
        "(v01)-[{a : 2,b : 1,count : 1L}]->(v01)" +
        "(v01)-[{a : 2,b : 3,count : 1L}]->(v01)" +
        "(v00)-[{a : 0,b : 1,count : 1L}]->(v01)" +
        "(v00)-[{a : 0,b : 2,count : 1L}]->(v01)" +
        "(v01)-[{a : 1,b : 2,count : 1L}]->(v00)" +
        "(v01)-[{a : 1,b : 3,count : 1L}]->(v00)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("a")
                .addEdgeGroupingKey("a")
                .addEdgeGroupingKey("b")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10))));

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMultipleVertexAndMultipleEdgeProperties() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("" +
        "input[" +
        "(v0 {a : 0,b : 0})" +
        "(v1 {a : 0,b : 1})" +
        "(v2 {a : 0,b : 1})" +
        "(v3 {a : 1,b : 0})" +
        "(v4 {a : 1,b : 1})" +
        "(v5 {a : 1,b : 0})" +
        "(v0)-[{a : 0,b : 1}]->(v1)" +
        "(v0)-[{a : 0,b : 2}]->(v2)" +
        "(v1)-[{a : 0,b : 3}]->(v2)" +
        "(v2)-[{a : 0,b : 2}]->(v3)" +
        "(v2)-[{a : 0,b : 1}]->(v3)" +
        "(v4)-[{a : 1,b : 2}]->(v2)" +
        "(v5)-[{a : 1,b : 3}]->(v2)" +
        "(v3)-[{a : 2,b : 3}]->(v4)" +
        "(v4)-[{a : 2,b : 1}]->(v5)" +
        "(v5)-[{a : 2,b : 0}]->(v3)" +
        "]"
    );

    loader.appendFromString("expected[" +
        "(v00 {a : 0,b : 0,count : 1L})" +
        "(v01 {a : 0,b : 1,count : 2L})" +
        "(v10 {a : 1,b : 0,count : 2L})" +
        "(v11 {a : 1,b : 1,count : 1L})" +
        "(v00)-[{a : 0,b : 1,count : 1L}]->(v01)" +
        "(v00)-[{a : 0,b : 2,count : 1L}]->(v01)" +
        "(v01)-[{a : 0,b : 3,count : 1L}]->(v01)" +
        "(v01)-[{a : 0,b : 1,count : 1L}]->(v10)" +
        "(v01)-[{a : 0,b : 2,count : 1L}]->(v10)" +
        "(v11)-[{a : 2,b : 1,count : 1L}]->(v10)" +
        "(v10)-[{a : 2,b : 3,count : 1L}]->(v11)" +
        "(v10)-[{a : 2,b : 0,count : 1L}]->(v10)" +
        "(v10)-[{a : 1,b : 3,count : 1L}]->(v01)" +
        "(v11)-[{a : 1,b : 2,count : 1L}]->(v01)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");
    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("a")
                .addVertexGroupingKey("b")
                .addEdgeGroupingKey("a")
                .addEdgeGroupingKey("b")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10))));

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexAndEdgePropertyWithAbsentValues() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "g3");

    loader.appendFromString("expected[" +
        "(dresden {city : \"Dresden\", count : 2L})" +
        "(others  {city : " + NULL_STRING + ", count : 1L})" +
        "(others)-[{since : 2013, count : 1L}]->(dresden)" +
        "(others)-[{since : " + NULL_STRING + ", count : 2L}]->(dresden)" +
        "(dresden)-[{since : 2014, count : 1L}]->(dresden)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .addVertexGroupingKey("city")
                    .addEdgeGroupingKey("since")
                    .addVertexAggregateFunction(new Count("count"))
                    .addEdgeAggregateFunction(new Count("count"))
                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexLabel() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStream(config);

    loader.appendFromString("expected[" +
        "(p:Person  {count : 6L})" +
        "(t:Tag     {count : 3L})" +
        "(f:Forum   {count : 2L})" +
        "(p)-[{count : 10L}]->(p)" +
        "(f)-[{count :  6L}]->(p)" +
        "(p)-[{count :  4L}]->(t)" +
        "(f)-[{count :  4L}]->(t)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .useVertexLabel(true)
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexLabelAndSingleVertexProperty() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    loader.appendFromString("expected[" +
        "(l:Person {city : \"Leipzig\", count : 2L})" +
        "(d:Person {city : \"Dresden\", count : 3L})" +
        "(b:Person {city : \"Berlin\",  count : 1L})" +
        "(d)-[{count : 2L}]->(d)" +
        "(d)-[{count : 3L}]->(l)" +
        "(l)-[{count : 2L}]->(l)" +
        "(l)-[{count : 1L}]->(d)" +
        "(b)-[{count : 2L}]->(d)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .useVertexLabel(true)
                .addVertexGroupingKey("city")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexLabelAndSingleVertexPropertyWithAbsentValue() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStream(config);

    loader.appendFromString("expected[" +
        "(pL:Person {city : \"Leipzig\", count : 2L})" +
        "(pD:Person {city : \"Dresden\", count : 3L})" +
        "(pB:Person {city : \"Berlin\",  count : 1L})" +
        "(t:Tag {city : " + NULL_STRING + ",   count : 3L})" +
        "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
        "(pD)-[{count : 2L}]->(pD)" +
        "(pD)-[{count : 3L}]->(pL)" +
        "(pL)-[{count : 2L}]->(pL)" +
        "(pL)-[{count : 1L}]->(pD)" +
        "(pB)-[{count : 2L}]->(pD)" +
        "(pB)-[{count : 1L}]->(t)" +
        "(pD)-[{count : 2L}]->(t)" +
        "(pL)-[{count : 1L}]->(t)" +
        "(f)-[{count : 3L}]->(pD)" +
        "(f)-[{count : 3L}]->(pL)" +
        "(f)-[{count : 4L}]->(t)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .useVertexLabel(true)
                .addVertexGroupingKey("city")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexLabelAndSingleEdgeProperty() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    loader.appendFromString("expected[" +
        "(p:Person {count : 6L})" +
        "(p)-[{since : 2014, count : 4L}]->(p)" +
        "(p)-[{since : 2013, count : 3L}]->(p)" +
        "(p)-[{since : 2015, count : 3L}]->(p)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .useVertexLabel(true)
                .addEdgeGroupingKey("since")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStream(config);

    loader.appendFromString("expected[" +
        "(p:Person  {count : 6L})" +
        "(t:Tag     {count : 3L})" +
        "(f:Forum   {count : 2L})" +
        "(p)-[{since : 2014, count : 4L}]->(p)" +
        "(p)-[{since : 2013, count : 3L}]->(p)" +
        "(p)-[{since : 2015, count : 3L}]->(p)" +
        "(f)-[{since : 2013, count : 1L}]->(p)" +
        "(p)-[{since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "(f)-[{since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "(f)-[{since : " + NULL_STRING + ", count : 5L}]->(p)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .useVertexLabel(true)
                .addEdgeGroupingKey("since")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexLabelAndSingleVertexAndSingleEdgeProperty() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    loader.appendFromString("expected[" +
        "(l:Person {city : \"Leipzig\", count : 2L})" +
        "(d:Person {city : \"Dresden\", count : 3L})" +
        "(b:Person {city : \"Berlin\",  count : 1L})" +
        "(d)-[{since : 2014, count : 2L}]->(d)" +
        "(d)-[{since : 2013, count : 2L}]->(l)" +
        "(d)-[{since : 2015, count : 1L}]->(l)" +
        "(l)-[{since : 2014, count : 2L}]->(l)" +
        "(l)-[{since : 2013, count : 1L}]->(d)" +
        "(b)-[{since : 2015, count : 2L}]->(d)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .useVertexLabel(true)
                .addVertexGroupingKey("city")
                .addEdgeGroupingKey("since")
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);

  }

  @Test
  public void testVertexAndEdgeLabel() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStream(config);

    loader.appendFromString("expected[" +
        "(p:Person  {count : 6L})" +
        "(t:Tag     {count : 3L})" +
        "(f:Forum   {count : 2L})" +
        "(f)-[:hasModerator {count :  2L}]->(p)" +
        "(p)-[:hasInterest  {count :  4L}]->(t)" +
        "(f)-[:hasMember    {count :  4L}]->(p)" +
        "(f)-[:hasTag       {count :  4L}]->(t)" +
        "(p)-[:knows        {count : 10L}]->(p)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .useVertexLabel(true)
                .useEdgeLabel(true)

                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexProperty() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    loader.appendFromString("expected[" +
        "(l:Person {city : \"Leipzig\", count : 2L})" +
        "(d:Person {city : \"Dresden\", count : 3L})" +
        "(b:Person {city : \"Berlin\",  count : 1L})" +
        "(d)-[:knows {count : 2L}]->(d)" +
        "(d)-[:knows {count : 3L}]->(l)" +
        "(l)-[:knows {count : 2L}]->(l)" +
        "(l)-[:knows {count : 1L}]->(d)" +
        "(b)-[:knows {count : 2L}]->(d)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .useVertexLabel(true)
                .useEdgeLabel(true)
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexAndEdgeLabelAndVertexAndSingleEdgeProperty() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

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

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .addEdgeGroupingKey("since")
                .useVertexLabel(true)
                .useEdgeLabel(true)
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );
    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexPropertyWithAbsentValue() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStream(config);

    loader.appendFromString("expected[" +
        "(pL:Person {city : \"Leipzig\", count : 2L})" +
        "(pD:Person {city : \"Dresden\", count : 3L})" +
        "(pB:Person {city : \"Berlin\", count : 1L})" +
        "(t:Tag   {city : " + NULL_STRING + ", count : 3L})" +
        "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
        "(pD)-[:knows {count : 2L}]->(pD)" +
        "(pD)-[:knows {count : 3L}]->(pL)" +
        "(pL)-[:knows {count : 2L}]->(pL)" +
        "(pL)-[:knows {count : 1L}]->(pD)" +
        "(pB)-[:knows {count : 2L}]->(pD)" +
        "(pB)-[:hasInterest {count : 1L}]->(t)" +
        "(pD)-[:hasInterest {count : 2L}]->(t)" +
        "(pL)-[:hasInterest {count : 1L}]->(t)" +
        "(f)-[:hasModerator {count : 1L}]->(pD)" +
        "(f)-[:hasModerator {count : 1L}]->(pL)" +
        "(f)-[:hasMember {count : 2L}]->(pD)" +
        "(f)-[:hasMember {count : 2L}]->(pL)" +
        "(f)-[:hasTag {count : 4L}]->(t)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addVertexGroupingKey("city")
                .useVertexLabel(true)
                .useEdgeLabel(true)
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgeProperty() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader
        .createEdgeStreamByGraphVariables(config, "g0", "g1", "g2");

    loader.appendFromString("expected[" +
        "(p:Person {count : 6L})" +
        "(p)-[:knows {since : 2013, count : 3L}]->(p)" +
        "(p)-[:knows {since : 2014, count : 4L}]->(p)" +
        "(p)-[:knows {since : 2015, count : 3L}]->(p)" +
        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addEdgeGroupingKey("since")
                .useVertexLabel(true)
                .useEdgeLabel(true)
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))
                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleEdgePropertyWithAbsentValue() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStream(config);

    loader.appendFromString("expected[" +
        "(p:Person  {count : 6L})" +
        "(t:Tag     {count : 3L})" +
        "(f:Forum   {count : 2L})" +
        "(p)-[:knows {since : 2014, count : 4L}]->(p)" +
        "(p)-[:knows {since : 2013, count : 3L}]->(p)" +
        "(p)-[:knows {since : 2015, count : 3L}]->(p)" +
        "(f)-[:hasModerator {since : 2013, count : 1L}]->(p)" +
        "(f)-[:hasModerator {since : " + NULL_STRING + ", count : 1L}]->(p)" +
        "(p)-[:hasInterest  {since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "(f)-[:hasMember    {since : " + NULL_STRING + ", count : 4L}]->(p)" +
        "(f)-[:hasTag       {since : " + NULL_STRING + ", count : 4L}]->(t)" +

        "]");

    var finalStream = edgeStream
        .callForStream(
            Grouping.createGrouping()
                .addEdgeGroupingKey("since")
                .useVertexLabel(true)
                .useEdgeLabel(true)
                .addVertexAggregateFunction(new Count("count"))
                .addEdgeAggregateFunction(new Count("count"))

                .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
        );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testVertexAndEdgeLabelAndSingleVertexAndSingleEdgePropertyWithAbsentValue()
      throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    var edgeStream = loader.createEdgeStream(config);

    loader.appendFromString("expected[" +
        "(pL:Person {city : \"Leipzig\", count : 2L})" +
        "(pD:Person {city : \"Dresden\", count : 3L})" +
        "(pB:Person {city : \"Berlin\", count : 1L})" +
        "(t:Tag   {city : " + NULL_STRING + ", count : 3L})" +
        "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
        "(pD)-[:knows {since : 2014, count : 2L}]->(pD)" +
        "(pD)-[:knows {since : 2013, count : 2L}]->(pL)" +
        "(pD)-[:knows {since : 2015, count : 1L}]->(pL)" +
        "(pL)-[:knows {since : 2014, count : 2L}]->(pL)" +
        "(pL)-[:knows {since : 2013, count : 1L}]->(pD)" +
        "(pB)-[:knows {since : 2015, count : 2L}]->(pD)" +
        "(pB)-[:hasInterest {since : " + NULL_STRING + ", count : 1L}]->(t)" +
        "(pD)-[:hasInterest {since : " + NULL_STRING + ", count : 2L}]->(t)" +
        "(pL)-[:hasInterest {since : " + NULL_STRING + ", count : 1L}]->(t)" +
        "(f)-[:hasModerator {since : 2013, count : 1L}]->(pD)" +
        "(f)-[:hasModerator {since : " + NULL_STRING + ", count : 1L}]->(pL)" +
        "(f)-[:hasMember {since : " + NULL_STRING + ", count : 2L}]->(pD)" +
        "(f)-[:hasMember {since : " + NULL_STRING + ", count : 2L}]->(pL)" +
        "(f)-[:hasTag {since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .addVertexGroupingKey("city")
                    .addEdgeGroupingKey("since")
                    .useVertexLabel(true)
                    .useEdgeLabel(true)
                    .addVertexAggregateFunction(new Count("count"))
                    .addEdgeAggregateFunction(new Count("count"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  //----------------------------------------------------------------------------
  // Tests for aggregate functions
  //----------------------------------------------------------------------------

  @Test
  public void testNoAggregate() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue)" +
        "(v01:Red)" +
        "(v00)-->(v00)" +
        "(v00)-->(v01)" +
        "(v01)-->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testCount() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {count : 3L})" +
        "(v01:Red  {count : 3L})" +
        "(v00)-[{count : 3L}]->(v00)" +
        "(v00)-[{count : 2L}]->(v01)" +
        "(v01)-[{count : 3L}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new Count("count"))
                    .addEdgeAggregateFunction(new Count("count"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testSum() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {sumA :  9})" +
        "(v01:Red  {sumA : 10})" +
        "(v00)-[{sumB : 5}]->(v00)" +
        "(v00)-[{sumB : 4}]->(v01)" +
        "(v01)-[{sumB : 5}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new SumProperty("a", "sumA"))
                    .addEdgeAggregateFunction(new SumProperty("b", "sumB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testSumWithMissingValue() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue)" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {sumA :  7})" +
        "(v01:Red  {sumA : 10})" +
        "(v00)-[{sumB : 3}]->(v00)" +
        "(v00)-[{sumB : 4}]->(v01)" +
        "(v01)-[{sumB : 5}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new SumProperty("a", "sumA"))
                    .addEdgeAggregateFunction(new SumProperty("b", "sumB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testSumWithMissingValues() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue)" +
        "(v1:Blue)" +
        "(v2:Blue)" +
        "(v3:Red)" +
        "(v4:Red)" +
        "(v5:Red)" +
        "(v0)-->(v1)" +
        "(v0)-->(v2)" +
        "(v1)-->(v2)" +
        "(v2)-->(v3)" +
        "(v2)-->(v3)" +
        "(v3)-->(v4)" +
        "(v4)-->(v5)" +
        "(v5)-->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {sumA :  " + NULL_STRING + "})" +
        "(v01:Red  {sumA :  " + NULL_STRING + "})" +
        "(v00)-[{sumB : " + NULL_STRING + "}]->(v00)" +
        "(v00)-[{sumB : " + NULL_STRING + "}]->(v01)" +
        "(v01)-[{sumB : " + NULL_STRING + "}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new SumProperty("a", "sumA"))
                    .addEdgeAggregateFunction(new SumProperty("b", "sumB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMin() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {minA : 2})" +
        "(v01:Red  {minA : 2})" +
        "(v00)-[{minB : 1}]->(v00)" +
        "(v00)-[{minB : 1}]->(v01)" +
        "(v01)-[{minB : 1}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new MinProperty("a", "minA"))
                    .addEdgeAggregateFunction(new MinProperty("b", "minB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMinWithMissingValue() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue)" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red)" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {minA : 3})" +
        "(v01:Red  {minA : 4})" +
        "(v00)-[{minB : 2}]->(v00)" +
        "(v00)-[{minB : 3}]->(v01)" +
        "(v01)-[{minB : 1}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new MinProperty("a", "minA"))
                    .addEdgeAggregateFunction(new MinProperty("b", "minB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMinWithMissingValues() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue)" +
        "(v1:Blue)" +
        "(v2:Blue)" +
        "(v3:Red)" +
        "(v4:Red)" +
        "(v5:Red)" +
        "(v0)-->(v1)" +
        "(v0)-->(v2)" +
        "(v1)-->(v2)" +
        "(v2)-->(v3)" +
        "(v2)-->(v3)" +
        "(v3)-->(v4)" +
        "(v4)-->(v5)" +
        "(v5)-->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {minA :  " + NULL_STRING + "})" +
        "(v01:Red  {minA :  " + NULL_STRING + "})" +
        "(v00)-[{minB : " + NULL_STRING + "}]->(v00)" +
        "(v00)-[{minB : " + NULL_STRING + "}]->(v01)" +
        "(v01)-[{minB : " + NULL_STRING + "}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new MinProperty("a", "minA"))
                    .addEdgeAggregateFunction(new MinProperty("b", "minB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMax() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {maxA : 4})" +
        "(v01:Red  {maxA : 4})" +
        "(v00)-[{maxB : 2}]->(v00)" +
        "(v00)-[{maxB : 3}]->(v01)" +
        "(v01)-[{maxB : 3}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
                    .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMaxWithMissingValue() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue)" +
        "(v3:Red)" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {maxA : 3})" +
        "(v01:Red  {maxA : 4})" +
        "(v00)-[{maxB : 2}]->(v00)" +
        "(v00)-[{maxB : 1}]->(v01)" +
        "(v01)-[{maxB : 1}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
                    .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMaxWithMissingValues() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue)" +
        "(v1:Blue)" +
        "(v2:Blue)" +
        "(v3:Red)" +
        "(v4:Red)" +
        "(v5:Red)" +
        "(v0)-->(v1)" +
        "(v0)-->(v2)" +
        "(v1)-->(v2)" +
        "(v2)-->(v3)" +
        "(v2)-->(v3)" +
        "(v3)-->(v4)" +
        "(v4)-->(v5)" +
        "(v5)-->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {maxA :  " + NULL_STRING + "})" +
        "(v01:Red  {maxA :  " + NULL_STRING + "})" +
        "(v00)-[{maxB : " + NULL_STRING + "}]->(v00)" +
        "(v00)-[{maxB : " + NULL_STRING + "}]->(v01)" +
        "(v01)-[{maxB : " + NULL_STRING + "}]->(v01)" +
        "]");

    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
                    .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

  @Test
  public void testMultipleAggregators() throws Exception {
    AsciiGraphLoader loader = AsciiGraphLoader.fromString("input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]");

    var edgeStream = loader.createEdgeStreamByGraphVariables(config, "input");

    loader.appendFromString("expected[" +
        "(v00:Blue {minA : 2,maxA : 4,sumA : 9,count : 3L})" +
        "(v01:Red  {minA : 2,maxA : 4,sumA : 10,count : 3L})" +
        "(v00)-[{minB : 1,maxB : 2,sumB : 5,count : 3L}]->(v00)" +
        "(v00)-[{minB : 1,maxB : 3,sumB : 4,count : 2L}]->(v01)" +
        "(v01)-[{minB : 1,maxB : 3,sumB : 5,count : 3L}]->(v01)" +
        "]");

    var test = Grouping.createGrouping()
        .useVertexLabel(true)
        .addVertexAggregateFunction(new MinProperty("a", "minA"))
        .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
        .addVertexAggregateFunction(new SumProperty("a", "sumA"))
        .addVertexAggregateFunction(new Count("count"))
        .addEdgeAggregateFunction(new MinProperty("b", "minB"))
        .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))
        .addEdgeAggregateFunction(new SumProperty("b", "sumB"))
        .addEdgeAggregateFunction(new Count("count"))

        .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)));
    var finalStream =
        edgeStream
            .callForStream(
                Grouping.createGrouping()
                    .useVertexLabel(true)
                    .addVertexAggregateFunction(new MinProperty("a", "minA"))
                    .addVertexAggregateFunction(new MaxProperty("a", "maxA"))
                    .addVertexAggregateFunction(new SumProperty("a", "sumA"))
                    .addVertexAggregateFunction(new Count("count"))
                    .addEdgeAggregateFunction(new MinProperty("b", "minB"))
                    .addEdgeAggregateFunction(new MaxProperty("b", "maxB"))
                    .addEdgeAggregateFunction(new SumProperty("b", "sumB"))
                    .addEdgeAggregateFunction(new Count("count"))

                    .buildWithWindow(TumblingEventTimeWindows.of(Time.milliseconds(10)))
            );

    var ecIt = finalStream.collect();
    var actualEcCol = new ArrayList<EdgeContainer>();
    while (ecIt.hasNext()) {
      actualEcCol.add(ecIt.next());
    }

    var expectedEcCol = loader.createEdgeContainersByGraphVariables("expected");

    validateEdgeContainerCollections(expectedEcCol, actualEcCol);
  }

}