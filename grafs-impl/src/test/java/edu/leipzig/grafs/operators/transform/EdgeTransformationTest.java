package edu.leipzig.grafs.operators.transform;


import static edu.leipzig.grafs.util.TestUtils.validateElementCollections;
import static edu.leipzig.grafs.util.TestUtils.validateIdEquality;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Element;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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


  static Element transformEdge(Element current) {
    current.setProperty("a", current.getPropertyValue("a").getInt() + 1);
    current.setProperty("b", current.getPropertyValue("b").getInt() - 1);
    return current;
  }

  @Test
  public void testIdEquality() throws Exception {
    AsciiGraphLoader loader = getLoaderFromString(testGraphString);

    Collection<Edge> referenceEdges = loader.getEdgesByGraphVariables("g0");
    List<GradoopId> expectedEdgeIds = referenceEdges.stream()
        .map(Element::getId)
        .collect(Collectors.toCollection(ArrayList::new));

    var inputStream = loader.createEdgeStreamByGraphVariables(getConfig(), "g0");
    var tripletResultIt = inputStream
        .transformEdges(EdgeTransformationTest::transformEdge)
        .collect();
    List<GradoopId> resultEdgeIds = new ArrayList<>();
    while (tripletResultIt.hasNext()) {
      var triplet = tripletResultIt.next();
      resultEdgeIds.add(triplet.getEdge().getId());
    }

    validateIdEquality(expectedEdgeIds, resultEdgeIds);
  }

  @Test
  public void testDataInequality() throws Exception {
    AsciiGraphLoader loader = getLoaderFromString(testGraphString);

    Collection<Edge> referenceEdges = loader.getEdgesByGraphVariables("g01");

    var inputStream = loader.createEdgeStreamByGraphVariables(getConfig(), "g0");
    var tripletResultIt = inputStream
        .transformEdges(EdgeTransformationTest::transformEdge)
        .collect();

    List<Edge> resultEdgeIds = new ArrayList<>();

    while (tripletResultIt.hasNext()) {
      var triplet = tripletResultIt.next();
      resultEdgeIds.add(triplet.getEdge());
    }

    validateDataInequality(referenceEdges, resultEdgeIds);
  }

  /**
   * Tests the data in the resulting graph.
   *
   * @throws Exception on failure
   */
  @Test
  public void testEdgeTransformation() throws Exception {
    AsciiGraphLoader loader = getLoaderFromString(testGraphString);

    Collection<Edge> expectedEdges = loader.getEdgesByGraphVariables("g01");
    Collection<Vertex> expectedVertices = loader.getVerticesByGraphVariables("g01");

    GraphStream original = loader.createEdgeStreamByGraphVariables(getConfig(), "g0");

    var tripletResultIt = original.transformEdges(EdgeTransformationTest::transformEdge).collect();

    Set<Edge> actualEdgeResult = new HashSet<>();
    Set<Vertex> actualVertexResult = new HashSet<>();
    while (tripletResultIt.hasNext()) {
      var triplet = tripletResultIt.next();
      actualEdgeResult.add(triplet.getEdge());
      actualVertexResult.add(triplet.getSourceVertex());
      actualVertexResult.add(triplet.getTargetVertex());
    }

    validateElementCollections(expectedEdges, actualEdgeResult);
    validateElementCollections(expectedVertices, actualVertexResult);
  }


}
