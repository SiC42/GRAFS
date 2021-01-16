package edu.leipzig.grafs.operators.grouping.logic;

import static edu.leipzig.grafs.util.TestUtils.GRADOOP_ID_VAL_8;
import static edu.leipzig.grafs.util.TestUtils.validateTripletCollections;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.*;

import edu.leipzig.grafs.operators.DummyCollector;
import edu.leipzig.grafs.operators.grouping.functions.AggregateFunction;
import edu.leipzig.grafs.operators.grouping.model.AggregateMode;
import edu.leipzig.grafs.operators.grouping.model.GroupingInformation;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import edu.leipzig.grafs.util.TestUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EdgeAggregationTest {


  @Test
  void flatMap_testSimpleAggregation() {
    var groupInfo = new GroupingInformation();
    groupInfo.addKey("n");
    var aggMap = new HashSet<AggregateFunction>();
    aggMap.add(TestUtils.INT_ADD_FUNC.apply("e"));
    var expectedGraphId = GRADOOP_ID_VAL_8;
    var vertexAggregation = new EdgeAggregation<>(groupInfo, aggMap, expectedGraphId);

    var collector = new DummyCollector();
    var loader = AsciiGraphLoader.fromString(
        "(a18 {n : \"A\", a : 18})," +
            "(c20 {n : \"B\", a : 20})," +
            "(a18)-[{n: \"Z\", e: 5}]->(c20)," +
            "(a18)-[{n: \"Z\", e: 6}]->(c20)," +
            "(a18)-[{n: \"Z\", e: 7}]->(c20),"
    );

    var tripletList = new ArrayList<>(loader.createTriplets());

    vertexAggregation.process("", null, tripletList, collector);

    // create expected output
    var expectedOutputLoader = AsciiGraphLoader.fromString(
        "(a18 {n : \"A\", a : 18})," +
            "(c20 {n : \"B\", a : 20})," +
            "(a18)-[{n: \"Z\", e: 18}]->(c20)"
    );

    var expectedTripletCol = expectedOutputLoader.createTriplets();
    var actualTripletCol = collector.getCollected();

    //verify collector was called with the right output
    validateTripletCollections(expectedTripletCol, actualTripletCol);
    for(var triplet : actualTripletCol){
      assertThat(triplet.getEdge().getGraphIds(), contains(GRADOOP_ID_VAL_8));
      assertThat(triplet.getSourceVertex().getGraphIds(), contains(GRADOOP_ID_VAL_8));
      assertThat(triplet.getTargetVertex().getGraphIds(), contains(GRADOOP_ID_VAL_8));
    }
  }

}