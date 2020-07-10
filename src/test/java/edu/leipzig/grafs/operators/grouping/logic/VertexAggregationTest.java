package edu.leipzig.grafs.operators.grouping.logic;

import static org.mockito.Mockito.mock;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.TestUtils;
import java.util.HashSet;
import java.util.Set;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

class VertexAggregationTest {

  @Test
  void flatMap() {
    GroupingInformation egi = new GroupingInformation();
    egi.addKey("n");
    AggregationMapping am = new AggregationMapping();
    am.addAggregationForProperty("a", TestUtils.INT_ADD_FUNC);

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