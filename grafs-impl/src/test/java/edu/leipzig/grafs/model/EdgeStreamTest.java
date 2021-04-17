package edu.leipzig.grafs.model;

import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.FlinkConfig;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

class EdgeStreamTest {

  final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();
  GraphStream graphStream;


  public EdgeStreamTest() {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
    FlinkConfig config = new FlinkConfigBuilder(env)
        .withWaterMarkStrategy(WatermarkStrategy
            .<Triplet<Vertex, Edge>>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
    graphStream = loader.createGraphStream(config);
  }
}