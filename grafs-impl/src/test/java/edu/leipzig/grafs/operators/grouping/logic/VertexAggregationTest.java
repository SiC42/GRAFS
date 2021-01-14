package edu.leipzig.grafs.operators.grouping.logic;

import static edu.leipzig.grafs.util.TestUtils.validateTripletCollections;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.operators.DummyCollector;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.TestUtils;
import java.util.ArrayList;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class VertexAggregationTest {

  @Test
  void flatMap_testSimpleAggregationForSource() {
    var groupInfo = new GroupingInformation();
    groupInfo.addKey("n");
    var aggMap = new HashSet<AggregateFunction>();
    aggMap.add(TestUtils.INT_ADD_FUNC.apply("a"));

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

    var tripletList = new ArrayList<>(loader.createTriplets());

    vertexAggregation.process("", null, tripletList, collector);

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

    var expectedTripletCol = expectedOutputLoader.createTriplets();

    //verify collector was called with the right output
    validateTripletCollections(expectedTripletCol, collector.getCollected());
  }

  @Test
  void flatMap_testSimpleAggregationForTarget() {
    var groupInfo = new GroupingInformation();
    groupInfo.addKey("n");
    var aggMap = new HashSet<AggregateFunction>();
    aggMap.add(TestUtils.INT_ADD_FUNC.apply("a"));

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

    var tripletList = new ArrayList<>(loader.createTriplets());

    vertexAggregation.process("", null, tripletList, collector);

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

    var expectedTripletCol = expectedOutputLoader.createTriplets();

    //verify collector was called with the right output
    validateTripletCollections(expectedTripletCol, collector.getCollected());
  }

  @Test
  void flatMap_testSimpleAggregationWithInvertedEdge() {
    var groupInfo = new GroupingInformation();
    groupInfo.addKey("n");
    var aggMap = new HashSet<AggregateFunction>();
    aggMap.add(TestUtils.INT_ADD_FUNC.apply("a"));

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

    var tripletList = new ArrayList<>(loader.createTriplets());
    loader = AsciiGraphLoader.fromString(
        "(a25 {n : \"A\", a : 25})," +
            "(c20 {n : \"C\", a : 20})," +
            "(c20)-[]->(a25),"
    );

    var invertedTripletList = new ArrayList<>(loader.createTriplets());
    Triplet invertedTriplet = invertedTripletList.get(0);
    invertedTriplet = invertedTriplet.createReverseTriplet();
    tripletList.add(invertedTriplet);

    vertexAggregation.process("", null, tripletList, collector);

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

    var expectedTripletCol = expectedOutputLoader.createTriplets();

    loader = AsciiGraphLoader.fromString(
        "(a25 {n : \"A\", a : 25})," +
            "(c20 {n : \"C\", a : 20})," +
            "(c20)-[]->(a25)"
    );

    var expectedInvertedTripletList = new ArrayList<>(loader.createTriplets());
    Triplet expectedInvertedTriplet = expectedInvertedTripletList.get(0);
    expectedInvertedTriplet = expectedInvertedTriplet.createReverseTriplet();
    expectedTripletCol.add(expectedInvertedTriplet);

    //verify collector was called with the right output
    validateTripletCollections(expectedTripletCol, collector.getCollected());
  }

}