package edu.leipzig.grafs.operators.matching.logic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import edu.leipzig.grafs.operators.DummyCollector;
import org.junit.jupiter.api.Test;

class IsomorphismMatchingProcessTest extends MatchingTestBase {


  @Test
  public void testProcessQuery() {
    var appendDsGraph = "iso {}["
        // iso-edges
        // from blue
        + "(v1)-[e1]->(v2)"
        + "(v2)-[e2]->(v1)"
        + "(v2)-[e3]->(v3)"
        + "(v2)-[e4]->(v4)"
        + "(v2)-[e5]->(v5)]";
    graphLoader.appendFromString(appendDsGraph);
    var isoMatching = new IsomorphismMatchingProcess<>(queryGraph);
    var collector = new DummyCollector();
    isoMatching.processQuery(graph, collector);
    var actualTriplets = collector.getCollected();
    var expectedTriplets = graphLoader.createTripletsByGraphVariables("iso");
    assertThat(actualTriplets, containsInAnyOrder(expectedTriplets.toArray()));
  }

}