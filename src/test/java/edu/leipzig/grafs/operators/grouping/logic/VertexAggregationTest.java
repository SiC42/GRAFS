package edu.leipzig.grafs.operators.grouping.logic;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.TestUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class VertexAggregationTest {

  @Test
  void flatMap() {
    var groupInfo = new GroupingInformation();
    groupInfo.addKey("n");
    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty("a", TestUtils.INT_ADD_FUNC);

    var vertexAggregation = new VertexAggregation<>(groupInfo, aggMap,
        AggregateMode.SOURCE);

    Collector<EdgeContainer> collector = mock(Collector.class);
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(
        "(a18 {n : \"A\", a : 18})," +
            "(a20 {n : \"A\", a : 20})," +
            "(b17 {n : \"B\", a : 17})," +
            "(b19 {n : \"B\", a : 19})," +
            "(c20 {n : \"C\", a : 20})," +
            "(a18)-[]->(b17)," +
            "(a18)-[]->(c20)," +
            "(a20)-[]->(b19),"
    );
    var edgeList = new ArrayList<>(loader.createEdgeContainers());

    vertexAggregation.process("", null, List.of(edgeList.get(0)), collector);



    //verify collector was called with the right output
    Mockito.verify(collector, times(1)).collect(edgeList.get(0));
  }
}