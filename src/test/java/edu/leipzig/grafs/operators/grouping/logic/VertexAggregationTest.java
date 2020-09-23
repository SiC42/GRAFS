package edu.leipzig.grafs.operators.grouping.logic;

import static edu.leipzig.grafs.util.TestUtils.validateEdgeContainerCollections;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.DummyCollector;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.AggregationMapping;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.TestUtils;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

class VertexAggregationTest {

  @Test
  void flatMap_testSimpleAggregationForSource() {
    var groupInfo = new GroupingInformation();
    groupInfo.addKey("n");
    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty("a", TestUtils.INT_ADD_FUNC.apply("a"));

    var vertexAggregation = new VertexAggregation<>(groupInfo, aggMap,
        AggregateMode.SOURCE);

    DummyCollector collector = new DummyCollector();
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(
        "(a18 {n : \"A\", a : 18})," +
            "(a20 {n : \"A\", a : 20})," +
            "(a25 {n : \"A\", a : 25})," +
            "(b17 {n : \"B\", a : 17})," +
            "(b19 {n : \"B\", a : 19})," +
            "(c20 {n : \"C\", a : 20})," +
            "(a18)-[]->(b17)," +
            "(a18)-[]->(c20)," +
            "(a20)-[]->(b19),"
    );

    var edgeList = new ArrayList<>(loader.createEdgeContainers());

    vertexAggregation.process("", null, edgeList, collector);

    // create expected output
    AsciiGraphLoader expectedOutputLoader = AsciiGraphLoader.fromString(
        "(a38 {n : \"A\", a : 38})," +
            "(b17 {n : \"B\", a : 17})," +
            "(b19 {n : \"B\", a : 19})," +
            "(c20 {n : \"C\", a : 20})," +
            "(a38)-[]->(b17)," +
            "(a38)-[]->(c20)," +
            "(a38)-[]->(b19),"
    );

    var expectedEcCol = expectedOutputLoader.createEdgeContainers();

    //verify collector was called with the right output
    validateEdgeContainerCollections(expectedEcCol, collector.getCollected());
  }

  @Test
  void flatMap_testSimpleAggregationForTarget() {
    var groupInfo = new GroupingInformation();
    groupInfo.addKey("n");
    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty("a", TestUtils.INT_ADD_FUNC.apply("a"));

    var vertexAggregation = new VertexAggregation<>(groupInfo, aggMap,
        AggregateMode.TARGET);

    DummyCollector collector = new DummyCollector();
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(
        "(a18 {n : \"A\", a : 18})," +
            "(a20 {n : \"A\", a : 20})," +
            "(b17 {n : \"B\", a : 17})," +
            "(b19 {n : \"B\", a : 19})," +
            "(c20 {n : \"C\", a : 20})," +
            "(a18)-[]->(b17)," +
            "(c20)-[]->(b17)," +
            "(a20)-[]->(b19),"
    );

    var edgeList = new ArrayList<>(loader.createEdgeContainers());

    vertexAggregation.process("", null, edgeList, collector);

    // create expected output
    AsciiGraphLoader expectedOutputLoader = AsciiGraphLoader.fromString(
        "(a18 {n : \"A\", a : 18})," +
            "(a20 {n : \"A\", a : 20})," +
            "(b36 {n : \"B\", a : 36})," +
            "(c20 {n : \"C\", a : 20})," +
            "(a18)-[]->(b36)," +
            "(c20)-[]->(b36)," +
            "(a20)-[]->(b36),"
    );

    var expectedEcCol = expectedOutputLoader.createEdgeContainers();

    //verify collector was called with the right output
    validateEdgeContainerCollections(expectedEcCol, collector.getCollected());
  }

  @Test
  void flatMap_testSimpleAggregationWithInvertedEdge() {
    var groupInfo = new GroupingInformation();
    groupInfo.addKey("n");
    var aggMap = new AggregationMapping();
    aggMap.addAggregationForProperty("a", TestUtils.INT_ADD_FUNC.apply("a"));

    var vertexAggregation = new VertexAggregation<>(groupInfo, aggMap,
        AggregateMode.SOURCE);

    DummyCollector collector = new DummyCollector();
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
    loader = AsciiGraphLoader.fromString(
        "(a25 {n : \"A\", a : 25})," +
            "(c20 {n : \"C\", a : 20})," +
            "(c20)-[]->(a25),"
    );

    var invertedEdgeList = new ArrayList<>(loader.createEdgeContainers());
    EdgeContainer invertedEc = invertedEdgeList.get(0);
    invertedEc = invertedEc.createReverseEdgeContainer();
    edgeList.add(invertedEc);

    vertexAggregation.process("", null, edgeList, collector);

    // create expected output
    AsciiGraphLoader expectedOutputLoader = AsciiGraphLoader.fromString(
        "(a63 {n : \"A\", a : 63})," +
            "(b17 {n : \"B\", a : 17})," +
            "(b19 {n : \"B\", a : 19})," +
            "(c20 {n : \"C\", a : 20})," +
            "(a63)-[]->(b17)," +
            "(a63)-[]->(c20)," +
            "(a63)-[]->(b19)"
    );

    var expectedEcCol = expectedOutputLoader.createEdgeContainers();

    loader = AsciiGraphLoader.fromString(
        "(a25 {n : \"A\", a : 25})," +
            "(c20 {n : \"C\", a : 20})," +
            "(c20)-[]->(a25)"
    );

    var expectedInvertedEdgeList = new ArrayList<>(loader.createEdgeContainers());
    EdgeContainer expectedInvertedEc = expectedInvertedEdgeList.get(0);
    expectedInvertedEc = expectedInvertedEc.createReverseEdgeContainer();
    expectedEcCol.add(expectedInvertedEc);

    //verify collector was called with the right output
    validateEdgeContainerCollections(expectedEcCol, collector.getCollected());
  }

}