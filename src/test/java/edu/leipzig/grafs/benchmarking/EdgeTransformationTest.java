package edu.leipzig.grafs.benchmarking;


import static edu.leipzig.grafs.util.TestUtils.validateIdEquality;

import edu.leipzig.grafs.benchmarking.transform.BenchmarkEdgeTransformation;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.operators.transform.TransformationBaseTest;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.gradoop.common.model.impl.id.GradoopId;
import org.junit.jupiter.api.Test;

public class EdgeTransformationTest extends TransformationBaseTest {

  final String testGraphString = "" +
      "g0:A  { a : 1 } [(:A { a : 1, b : 2 })-[:a { a : 1, b : 2 }]->(:B { c : 2 })]" +
      "g1:B  { a : 2 } [(:A { a : 2, b : 2 })-[:a { a : 3, b : 4 }]->(:B { c : 3 })]" +
      // edge transformation
      "g01:A  { a : 1 } [(:A { a : 1, b : 2 })-[:a { a : 2, b : 1 }]->(:B { c : 2 })]" +
      "g11:B  { a : 2 } [(:A { a : 2, b : 2 })-[:a { a : 4, b : 3 }]->(:B { c : 3 })]";


  static Edge transformEdge(Edge current) {
    current.setProperty("a", current.getPropertyValue("a").getInt() + 1);
    current.setProperty("b", current.getPropertyValue("b").getInt() - 1);
    return current;
  }

  @Test
  public void test() throws Exception {
    AsciiGraphLoader loader = getLoaderFromString(testGraphString);

    Collection<Edge> referenceEdges = loader.getEdgesByGraphVariables("g0");
    List<GradoopId> expectedEdgeIds = referenceEdges.stream()
        .map(Element::getId)
        .collect(Collectors.toCollection(ArrayList::new));

    var inputStream = loader.createEdgeStreamByGraphVariables(getConfig(), "g0");
    var ecResultIterator = inputStream
        .callForStream(new BenchmarkEdgeTransformation(EdgeTransformationTest::transformEdge))
        .collect();
    List<GradoopId> resultEdgeIds = new ArrayList<>();
    while (ecResultIterator.hasNext()) {
      var ec = ecResultIterator.next();
      resultEdgeIds.add(ec.getEdge().getId());
    }

    validateIdEquality(expectedEdgeIds, resultEdgeIds);
  }



}
