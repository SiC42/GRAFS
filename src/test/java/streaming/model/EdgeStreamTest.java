package streaming.model;

import java.util.Collection;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;
import streaming.operators.grouping.functions.PropertiesAggregationFunction;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;
import streaming.util.AsciiGraphLoader;

class EdgeStreamTest {

  final StreamExecutionEnvironment env =
      StreamExecutionEnvironment.getExecutionEnvironment();
  EdgeStream edgeStream;


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

    Collection<EdgeContainer> edges = loader.createEdgeContainers();
    DataStream<EdgeContainer> m = env.fromCollection(edges);

    edgeStream = new EdgeStream(m);
  }

  @Test
  void groupBy() throws Exception {
    GroupingInformation vertexEgi = new GroupingInformation();
    vertexEgi.groupingKeys.add("n");
    AggregationMapping am = new AggregationMapping();
    var identity = new PropertyValue();
    identity.setInt(0);
    am.addAggregationForProperty("a",
        new PropertiesAggregationFunction(identity, (PropertyValue pV1, PropertyValue pV2) -> {
          var newVal = new PropertyValue();
          newVal.setInt(pV1.getInt() + pV2.getInt());
          return newVal;
        }));
    edgeStream.groupBy(vertexEgi, am, null, null).print();
    env.execute();
  }
}