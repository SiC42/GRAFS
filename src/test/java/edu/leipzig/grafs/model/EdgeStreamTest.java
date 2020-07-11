package edu.leipzig.grafs.model;

import edu.leipzig.grafs.operators.grouping.Grouping;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.operators.grouping.model.PropertiesAggregationFunction;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;

class EdgeStreamTest {

  final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();
  EdgeStream edgeStream;


  public EdgeStreamTest() {

    AsciiGraphLoader loader = AsciiGraphLoader.fromString(
        "(a18 {n : \"A\", a : 18})," +
            "(a20 {n : \"A\", a : 20})," +
            "(a25 {n : \"A\", a : 25})," +
            "(b17 {n : \"B\", a : 17})," +
            "(b19 {n : \"B\", a : 19})," +
            "(c20 {n : \"C\", a : 20})," +
            "(a18)-[]->(b17)," +
            "(a18)-[]->(c20)," +
            "(c20)-[]->(a25)," +
            "(c20)-[]->(b17)," +
            "(a20)-[]->(b19),"
    );
    FlinkConfig config = FlinkConfig.buildNewConfig(env)
        .build();
    edgeStream = loader.createEdgeStream(config);
  }

  @Test
  void groupBy() throws Exception {
    GroupingInformation vertexEgi = new GroupingInformation();
    vertexEgi.addKey("n");
    AggregationMapping am = new AggregationMapping();
    var identity = new PropertyValue();
    identity.setInt(0);
    am.addAggregationForProperty("a",
        new PropertiesAggregationFunction(identity, (PropertyValue pV1, PropertyValue pV2) -> {
          var newVal = new PropertyValue();
          newVal.setInt(pV1.getInt() + pV2.getInt());
          return newVal;
        }));
    edgeStream.callForStream(
        Grouping.createGrouping()
            .withVertexGrouping(vertexEgi, am)
            .buildWithWindow(TumblingProcessingTimeWindows.of(Time.milliseconds(10))))
        .print();
    env.execute();
  }
}