package edu.leipzig.grafs.benchmarking;

import static edu.leipzig.grafs.util.TestUtils.validateElementCollections;

import edu.leipzig.grafs.benchmarking.transform.BenchmarkVertexTransformation;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.transform.TransformationBaseTest;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class BenchmarkVertexTransformationTest extends TransformationBaseTest {

  final String testGraphString = "" +
      "g0:A  { a : 1 } [(:A { a : 1, b : 2 })-[:a { a : 1, b : 2 }]->(:B { c : 2 })]" +
      "g1:B  { a : 2 } [(:A { a : 2, b : 6 })-[:a { a : 3, b : 4 }]->(:B { c : 3 })]" +
      // vertex transformation
      "g01:A  { a : 1 } [(:A { a : 2, b : 1 })-[:a { a : 1, b : 2 }]->(:B { c : 2, d : 2 })]" +
      "g11:B  { a : 2 } [(:A { a : 3, b : 5 })-[:a { a : 3, b : 4 }]->(:B { c : 2, d : 2 })]";


  static Vertex transformVertex(Vertex current) {
    current.setLabel(current.getLabel());
    if (current.getLabel().equals("A")) {
      current.setProperty("a", current.getPropertyValue("a").getInt() + 1);
      current.setProperty("b", current.getPropertyValue("b").getInt() - 1);
    } else if (current.getLabel().equals("B")) {
      current.setProperty("d", current.getPropertyValue("c"));
    }
    return current;
  }


  @Test
  public void test() throws Exception {
    AsciiGraphLoader loader = getLoaderFromString(testGraphString);

    Collection<Edge> expectedEdges = loader.getEdgesByGraphVariables("g01");
    Collection<Vertex> expectedVertices = loader.getVerticesByGraphVariables("g01");

    EdgeStream original = loader.createEdgeStreamByGraphVariables(getConfig(), "g0");

    var result = original
        .callForStream(
            new BenchmarkVertexTransformation(BenchmarkVertexTransformationTest::transformVertex))
        .collect();

    Set<Edge> actualEdgeResult = new HashSet<>();
    Set<Vertex> actualVertexResult = new HashSet<>();
    while (result.hasNext()) {
      var ec = result.next();
      actualEdgeResult.add(ec.getEdge());
      actualVertexResult.add(ec.getSourceVertex());
      actualVertexResult.add(ec.getTargetVertex());
    }

    validateElementCollections(expectedEdges, actualEdgeResult);
    validateElementCollections(expectedVertices, actualVertexResult);
  }

}