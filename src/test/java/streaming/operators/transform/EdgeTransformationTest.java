package streaming.operators.transform;


import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static streaming.util.TestUtils.validateElementCollections;
import static streaming.util.TestUtils.validateIdEquality;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.junit.jupiter.api.Test;
import streaming.model.Edge;
import streaming.model.EdgeContainer;
import streaming.model.EdgeStream;
import streaming.model.Element;
import streaming.model.Vertex;
import streaming.operators.OperatorTestBase;
import streaming.util.AsciiGraphLoader;

public class EdgeTransformationTest extends OperatorTestBase {

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

  public static void validateElementsDifference(Element originalElement,
      Element actualElement) {

    Properties originalProperties = Properties.create();
    originalElement.getProperties().forEach(originalProperties::set);
    Properties actualProperties = Properties.create();
    actualElement.getProperties().forEach(actualProperties::set);

    for (var originalProp : originalProperties) {
      var originalKey = originalProp.getKey();
      if (actualProperties.containsKey(originalKey)) {
        assertNotEquals(originalProp, actualProperties.get(originalKey));
        actualProperties.remove(originalKey);
      }
    }
    for (var actualProp : actualProperties) {
      var actualKey = actualProp.getKey();
      if (actualProperties.containsKey(actualKey)) {
        assertNotEquals(originalProperties.get(actualKey), actualProp);
      }
    }
  }

  @Test
  public void testIdEquality() throws Exception {
    AsciiGraphLoader loader = getLoaderFromString(testGraphString);

    var referenceCollection = loader.createEdgeContainersByGraphVariables("g0");
    List<GradoopId> expectedEdgeIds = referenceCollection.stream()
        .map(ec -> ec.getEdge().getId())
        .collect(Collectors.toCollection(ArrayList::new));

    var inputStream = loader.createEdgeStreamByGraphVariables(getConfig(), "g0");
    var ecResultIterator = inputStream
        .transformEdges(EdgeTransformationTest::transformEdge)
        .collect();
    List<GradoopId> resultEdgeIds = new ArrayList<>();
    while (ecResultIterator.hasNext()) {
      var ec = ecResultIterator.next();
      resultEdgeIds.add(ec.getEdge().getId());
    }

    validateIdEquality(expectedEdgeIds, resultEdgeIds);
  }

  @Test
  public void testDataInequality() throws Exception {
    AsciiGraphLoader loader = getLoaderFromString(testGraphString);

    var referenceCollection = loader.createEdgeContainersByGraphVariables("g0");
    List<Edge> referenceEdges = referenceCollection.stream()
        .map(EdgeContainer::getEdge)
        .collect(Collectors.toList());

    var inputStream = loader.createEdgeStreamByGraphVariables(getConfig(), "g0");
    var ecResultIterator = inputStream
        .transformEdges(EdgeTransformationTest::transformEdge)
        .collect();

    List<Edge> resultEdgeIds = new ArrayList<>();

    while (ecResultIterator.hasNext()) {
      var ec = ecResultIterator.next();
      resultEdgeIds.add(ec.getEdge());
    }

    validateDataInequality(referenceEdges, resultEdgeIds);
  }

  /**
   * Checks if two collections contain the same EPGM elements in terms of data (i.e. label and
   * properties).
   *
   * @param expectedCollection first collection
   * @param actualCollection   second collection
   */
  private void validateDataInequality(Collection<? extends Element> expectedCollection,
      Collection<? extends Element> actualCollection) {

    List<? extends Element> list1 = new ArrayList<>(expectedCollection);
    List<? extends Element> list2 = new ArrayList<>(actualCollection);
    final Comparator<Element> ID_COMPARATOR = Comparator.comparing(Element::getId);
    list1.sort(ID_COMPARATOR);
    list2.sort(ID_COMPARATOR);

    Iterator<? extends Element> it1 = list1.iterator();
    Iterator<? extends Element> it2 = list2.iterator();

    while (it1.hasNext()) {
      validateElementsDifference(
          it1.next(),
          it2.next());
    }
  }

  /**
   * Tests the data in the resulting graph.
   *
   * @throws Exception on failure
   */
  @Test
  public void testEdgeOnlyTransformation() throws Exception {
    AsciiGraphLoader loader = getLoaderFromString(testGraphString);

    var expectedEcList = loader.createEdgeContainersByGraphVariables("g01");
    Set<Edge> expectedEdgeResult = expectedEcList.stream()
        .map(EdgeContainer::getEdge)
        .collect(Collectors.toSet());
    Set<Vertex> expectedVertexResult = new HashSet<>();
    expectedEcList.forEach(ec -> {
      expectedVertexResult.add(ec.getSourceVertex());
      expectedVertexResult.add(ec.getTargetVertex());
    });

    EdgeStream original = loader.createEdgeStreamByGraphVariables(getConfig(), "g0");

    var result = original.transformEdges(EdgeTransformationTest::transformEdge).collect();

    Set<Edge> actualEdgeResult = new HashSet<>();
    Set<Vertex> actualVertexResult = new HashSet<>();
    while (result.hasNext()) {
      var ec = result.next();
      actualEdgeResult.add(ec.getEdge());
      actualVertexResult.add(ec.getSourceVertex());
      actualVertexResult.add(ec.getTargetVertex());
    }

    validateElementCollections(expectedEdgeResult, actualEdgeResult);
    validateElementCollections(expectedVertexResult, actualVertexResult);
  }


}
