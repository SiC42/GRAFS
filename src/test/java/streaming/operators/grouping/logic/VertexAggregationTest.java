package streaming.operators.grouping.logic;

import static org.mockito.Mockito.mock;

import java.util.HashSet;
import java.util.Set;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.junit.jupiter.api.Test;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.operators.grouping.model.AggregateMode;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;
import streaming.operators.grouping.model.PropertiesAggregationFunction;
import streaming.util.AsciiGraphLoader;

class VertexAggregationTest {

  @Test
  void flatMap() {
    GroupingInformation egi = new GroupingInformation();
    egi.groupingKeys.add("n");
    AggregationMapping am = new AggregationMapping();
    var identity = new PropertyValue();
    identity.setDouble(0);
    am.addAggregationForProperty("a",
        new PropertiesAggregationFunction(identity, (PropertyValue pV1, PropertyValue pV2) -> {
          var newVal = new PropertyValue();
          newVal.setDouble(pV1.getDouble() + pV2.getDouble());
          return newVal;
        }));

    VertexAggregation incrementer = new VertexAggregation(egi, am,
        AggregateMode.SOURCE);

    Collector<Edge> collector = mock(Collector.class);
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(
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
    Set<EdgeContainer> edgeSet = new HashSet<>(loader.createEdgeContainers());

    // call the methods that you have implemented
    //incrementer.flatMap(2L, collector);

    //verify collector was called with the right output
    //Mockito.verify(collector, times(1)).collect(3L);
  }
}