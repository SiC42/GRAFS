package streaming.model;

import java.util.Collection;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

    AsciiGraphLoader loader = new AsciiGraphLoader();

    Collection<EdgeContainer> edges = loader.loadFromString(
        "(a18 {n : \"A\", a : \"18\"})," +
            "(a20 {n : \"A\", a : \"20\"})," +
            "(a25 {n : \"A\", a : \"25\"})," +
            "(b17 {n : \"B\", a : \"17\"})," +
            "(b19 {n : \"B\", a : \"19\"})," +
            "(c20 {n : \"C\", a : \"20\"})," +
            "(a18)-[]->(b17)," +
            "(a18)-[]->(c20)," +
            "(c20)-[]->(a25)," +
            "(c20)-[]->(b17)," +
            "(a20)-[]->(b19),"
    );
    DataStream<EdgeContainer> m = env.fromCollection(edges);

    edgeStream = new EdgeStream(m);
  }

  @Test
  void groupBy() throws Exception {
    GroupingInformation vertexEgi = new GroupingInformation();
    vertexEgi.groupingKeys.add("n");
    AggregationMapping am = new AggregationMapping();
    am.addAggregationForProperty("a",
        new PropertiesAggregationFunction("0", (String pV1, String pV2) -> String
            .valueOf(Double.parseDouble(pV1) + Double.parseDouble(pV2))));
    edgeStream.groupBy(vertexEgi, am, null, null).print();
    env.execute();
  }
}