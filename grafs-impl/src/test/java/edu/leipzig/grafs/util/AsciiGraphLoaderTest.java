package edu.leipzig.grafs.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Vertex;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.util.GradoopConstants;
import org.junit.jupiter.api.Test;

class AsciiGraphLoaderTest {


  @Test
  public void testFromString() {
    AsciiGraphLoader asciiGraphLoader = AsciiGraphLoader.fromString("[()-->()]");

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testFromFile() throws Exception {
    String file = URLDecoder.decode(
        getClass().getResource("/data/gdl/example.gdl").getFile(), StandardCharsets.UTF_8.name());
    AsciiGraphLoader asciiGraphLoader = AsciiGraphLoader.fromFile(file);

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testGetVertices() {
    AsciiGraphLoader asciiGraphLoader = AsciiGraphLoader.fromString("[()]");

    validateCollections(asciiGraphLoader, 1, 1, 0);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (Vertex vertex : asciiGraphLoader.getVertices()) {
      assertEquals(GradoopConstants.DEFAULT_VERTEX_LABEL, vertex.getLabel(),
          "Vertex has wrong label");
    }
  }


  @Test
  public void testGetVertexByVariable() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader.fromString("(a)");

    validateCollections(asciiGraphLoader, 0, 1, 0);
    validateCaches(asciiGraphLoader, 0, 1, 0);

    Vertex v = asciiGraphLoader.getVertexByVariable("a");
    assertEquals(GradoopConstants.DEFAULT_VERTEX_LABEL, v.getLabel(), "Vertex has wrong label");
    assertNotNull(v, "Vertex was null");
  }

  @Test
  public void testGetVerticesByVariables() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader
            .fromString("[(a),(b),(a)]");

    validateCollections(asciiGraphLoader, 1, 2, 0);
    validateCaches(asciiGraphLoader, 0, 2, 0);

    Collection<Vertex> vertexs = asciiGraphLoader
        .getVerticesByVariables("a", "b");

    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Vertex b = asciiGraphLoader.getVertexByVariable("b");

    assertEquals(2, vertexs.size(), "Wrong number of vertices");
    assertTrue(vertexs.contains(a), "Vertex was not contained in result");
    assertTrue(vertexs.contains(b), "Vertex was not contained in result");
  }

  @Test
  public void testGetVerticesByGraphIds() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader
            .fromString("g[(a),(b)],h[(a),(c)]");

    validateCollections(asciiGraphLoader, 2, 3, 0);
    validateCaches(asciiGraphLoader, 2, 3, 0);

    GradoopId g = asciiGraphLoader.getGraphIdByVariable("g");
    GradoopId h = asciiGraphLoader.getGraphIdByVariable("h");

    Collection<Vertex> vertexsG = asciiGraphLoader
        .getVerticesByGraphIds(GradoopIdSet.fromExisting(g));

    Collection<Vertex> vertexsH = asciiGraphLoader
        .getVerticesByGraphIds(GradoopIdSet.fromExisting(h));

    Collection<Vertex> vertexsGH = asciiGraphLoader
        .getVerticesByGraphIds(GradoopIdSet.fromExisting(g, h));

    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Vertex b = asciiGraphLoader.getVertexByVariable("b");
    Vertex c = asciiGraphLoader.getVertexByVariable("c");

    assertEquals(2, vertexsG.size(), "Wrong number of vertices");
    assertEquals(2, vertexsH.size(), "Wrong number of vertices");
    assertEquals(3, vertexsGH.size(), "Wrong number of vertices");
    assertTrue(vertexsG.contains(a), "Vertex was not contained in graph");
    assertTrue(vertexsG.contains(b), "Vertex was not contained in graph");
    assertTrue(vertexsH.contains(a), "Vertex was not contained in graph");
    assertTrue(vertexsH.contains(c), "Vertex was not contained in graph");
    assertTrue(vertexsGH.contains(a), "Vertex was not contained in graph");
    assertTrue(vertexsGH.contains(b), "Vertex was not contained in graph");
    assertTrue(vertexsGH.contains(c), "Vertex was not contained in graph");
  }

  @Test
  public void testGetVerticesByGraphVariables() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader
            .fromString("g[(a),(b)],h[(a),(c)]");

    validateCollections(asciiGraphLoader, 2, 3, 0);
    validateCaches(asciiGraphLoader, 2, 3, 0);

    Collection<Vertex> verticesG = asciiGraphLoader
        .getVerticesByGraphVariables("g");

    Collection<Vertex> verticesH = asciiGraphLoader
        .getVerticesByGraphVariables("h");

    Collection<Vertex> verticesGH = asciiGraphLoader
        .getVerticesByGraphVariables("g", "h");

    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Vertex b = asciiGraphLoader.getVertexByVariable("b");
    Vertex c = asciiGraphLoader.getVertexByVariable("c");

    assertEquals(2, verticesG.size(), "Wrong number of vertices");
    assertEquals(2, verticesH.size(), "Wrong number of vertices");
    assertEquals(3, verticesGH.size(), "Wrong number of vertices");
    assertTrue(verticesG.contains(a), "Vertex was not contained in graph");
    assertTrue(verticesG.contains(b), "Vertex was not contained in graph");
    assertTrue(verticesH.contains(a), "Vertex was not contained in graph");
    assertTrue(verticesH.contains(c), "Vertex was not contained in graph");
    assertTrue(verticesGH.contains(a), "Vertex was not contained in graph");
    assertTrue(verticesGH.contains(b), "Vertex was not contained in graph");
    assertTrue(verticesGH.contains(c), "Vertex was not contained in graph");
  }

  @Test
  public void testGetEdges() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader.fromString("[()-->()]");

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (Edge edge : asciiGraphLoader.getEdges()) {
      assertEquals(GradoopConstants.DEFAULT_EDGE_LABEL, edge.getLabel(), "Edge has wrong label");
      assertNotNull(edge.getSourceId(), "Source ID was null");
      assertNotNull(edge.getTargetId(), "Target ID was null");
    }
  }

  @Test
  public void testGetEdgesByVariables() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader
            .fromString("[()-[e]->()<-[f]-()]");

    validateCollections(asciiGraphLoader, 1, 3, 2);
    validateCaches(asciiGraphLoader, 0, 0, 2);

    Collection<Edge> edges = asciiGraphLoader
        .getEdgesByVariables("e", "f");

    Edge e = asciiGraphLoader.getEdgeByVariable("e");
    Edge f = asciiGraphLoader.getEdgeByVariable("f");

    assertEquals(2, edges.size(), "Wrong number of edges");
    assertTrue(edges.contains(e), "Edge was not contained in result");
    assertTrue(edges.contains(f), "Edge was not contained in result");
  }

  @Test
  public void testGetEdgesByGraphIds() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader.fromString("g[()-[a]->()<-[b]-()],h[()-[c]->()-[d]->()]");

    validateCollections(asciiGraphLoader, 2, 6, 4);
    validateCaches(asciiGraphLoader, 2, 0, 4);

    GradoopId g = asciiGraphLoader.getGraphIdByVariable("g");
    GradoopId h = asciiGraphLoader.getGraphIdByVariable("h");

    Collection<Edge> edgesG = asciiGraphLoader
        .getEdgesByGraphIds(GradoopIdSet.fromExisting(g));

    Collection<Edge> edgesH = asciiGraphLoader
        .getEdgesByGraphIds(GradoopIdSet.fromExisting(h));

    Collection<Edge> edgesGH = asciiGraphLoader
        .getEdgesByGraphIds(GradoopIdSet.fromExisting(g, h));

    Edge a = asciiGraphLoader.getEdgeByVariable("a");
    Edge b = asciiGraphLoader.getEdgeByVariable("b");
    Edge c = asciiGraphLoader.getEdgeByVariable("c");
    Edge d = asciiGraphLoader.getEdgeByVariable("d");

    assertEquals(2, edgesG.size(), "Wrong number of edges");
    assertEquals(2, edgesH.size(), "Wrong number of edges");
    assertEquals(4, edgesGH.size(), "Wrong number of edges");
    assertTrue(edgesG.contains(a), "Edge was not contained in graph");
    assertTrue(edgesG.contains(b), "Edge was not contained in graph");
    assertTrue(edgesH.contains(c), "Edge was not contained in graph");
    assertTrue(edgesH.contains(d), "Edge was not contained in graph");
    assertTrue(edgesGH.contains(a), "Edge was not contained in graph");
    assertTrue(edgesGH.contains(b), "Edge was not contained in graph");
    assertTrue(edgesGH.contains(c), "Edge was not contained in graph");
    assertTrue(edgesGH.contains(d), "Edge was not contained in graph");
  }

  @Test
  public void testGetEdgesByGraphVariables() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader.fromString("g[()-[a]->()<-[b]-()],h[()-[c]->()-[d]->()]");

    validateCollections(asciiGraphLoader, 2, 6, 4);
    validateCaches(asciiGraphLoader, 2, 0, 4);

    Collection<Edge> edgesG = asciiGraphLoader
        .getEdgesByGraphVariables("g");

    Collection<Edge> edgesH = asciiGraphLoader
        .getEdgesByGraphVariables("h");

    Collection<Edge> edgesGH = asciiGraphLoader
        .getEdgesByGraphVariables("g", "h");

    Edge a = asciiGraphLoader.getEdgeByVariable("a");
    Edge b = asciiGraphLoader.getEdgeByVariable("b");
    Edge c = asciiGraphLoader.getEdgeByVariable("c");
    Edge d = asciiGraphLoader.getEdgeByVariable("d");

    assertEquals(2, edgesG.size(), "Wrong number of edges");
    assertEquals(2, edgesH.size(), "Wrong number of edges");
    assertEquals(4, edgesGH.size(), "Wrong number of edges");
    assertTrue(edgesG.contains(a), "Edge was not contained in graph");
    assertTrue(edgesG.contains(b), "Edge was not contained in graph");
    assertTrue(edgesH.contains(c), "Edge was not contained in graph");
    assertTrue(edgesH.contains(d), "Edge was not contained in graph");
    assertTrue(edgesGH.contains(a), "Edge was not contained in graph");
    assertTrue(edgesGH.contains(b), "Edge was not contained in graph");
    assertTrue(edgesGH.contains(c), "Edge was not contained in graph");
    assertTrue(edgesGH.contains(d), "Edge was not contained in graph");
  }

  @Test
  public void testGetEdgeContainers() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader.fromString("[()-->()]");

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    for (var ec : asciiGraphLoader.createEdgeContainers()) {
      assertEquals(GradoopConstants.DEFAULT_EDGE_LABEL, ec.getEdge().getLabel(),
          "Edge has wrong label");
      assertNotNull(ec.getEdge().getSourceId(), "Source ID was null");
      assertNotNull(ec.getEdge().getTargetId(), "Target ID was null");
    }
  }

  @Test
  public void testGetGraphIdCache() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader.fromString("g[()],h[()],[()]");

    validateCollections(asciiGraphLoader, 3, 3, 0);
    validateCaches(asciiGraphLoader, 2, 0, 0);

    GradoopId g = asciiGraphLoader.getGraphIdByVariable("g");
    GradoopId h = asciiGraphLoader.getGraphIdByVariable("h");

    GradoopId gCache = asciiGraphLoader.getGraphIdCache().get("g");
    GradoopId hCache = asciiGraphLoader.getGraphIdCache().get("h");

    assertEquals(g, gCache, "Graphs were not equal");
    assertEquals(h, hCache, "Graphs were not equal");
  }

  @Test
  public void testGetVertexCache() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader
            .fromString("(a),(b),()");

    validateCollections(asciiGraphLoader, 0, 3, 0);
    validateCaches(asciiGraphLoader, 0, 2, 0);

    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Vertex b = asciiGraphLoader.getVertexByVariable("b");

    Vertex aCache = asciiGraphLoader.getVertexCache().get("a");
    Vertex bCache = asciiGraphLoader.getVertexCache().get("b");

    assertEquals(a, aCache, "Vertices were not equal");
    assertEquals(b, bCache, "Vertices were not equal");
  }

  @Test
  public void testGetEdgeCache() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader
            .fromString("()-[e]->()<-[f]-()-->()");

    validateCollections(asciiGraphLoader, 0, 4, 3);
    validateCaches(asciiGraphLoader, 0, 0, 2);

    Edge e = asciiGraphLoader.getEdgeByVariable("e");
    Edge f = asciiGraphLoader.getEdgeByVariable("f");

    Edge eCache = asciiGraphLoader.getEdgeCache().get("e");
    Edge fCache = asciiGraphLoader.getEdgeCache().get("f");

    assertEquals(e, eCache, "Edges were not equal");
    assertEquals(f, fCache, "Edges were not equal");
  }

  @Test
  public void testAppendFromString() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader.fromString("[()-->()]");

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    asciiGraphLoader.appendFromString("[()-->()]");
    validateCollections(asciiGraphLoader, 2, 4, 2);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testAppendFromString2() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader.fromString("[()-->()]");

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 0, 0, 0);

    asciiGraphLoader.appendFromString("()-->()");
    validateCollections(asciiGraphLoader, 1, 4, 2);
    validateCaches(asciiGraphLoader, 0, 0, 0);
  }

  @Test
  public void testAppendFromStringWithVariables() {
    AsciiGraphLoader asciiGraphLoader =
        AsciiGraphLoader
            .fromString("g0[(a)-[e]->(b)]");

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 1, 2, 1);

    asciiGraphLoader.appendFromString("g1[(a)-[e]->(b)]");
    validateCollections(asciiGraphLoader, 2, 2, 1);
    validateCaches(asciiGraphLoader, 2, 2, 1);

    GradoopId g1 = asciiGraphLoader.getGraphIdByVariable("g0");
    GradoopId g2 = asciiGraphLoader.getGraphIdByVariable("g1");
    Vertex a = asciiGraphLoader.getVertexByVariable("a");
    Edge e = asciiGraphLoader.getEdgeByVariable("e");

    assertEquals(2, a.getGraphCount(), "Vertex has wrong graph count");
    assertTrue(a.getGraphIds().contains(g1), "Vertex was not in g1");
    assertTrue(a.getGraphIds().contains(g2), "Vertex was not in g2");

    assertEquals(2, e.getGraphCount(), "Edge has wrong graph count");
    assertTrue(a.getGraphIds().contains(g1), "Edge was not in g1");
    assertTrue(a.getGraphIds().contains(g2), "Edge was not in g2");
  }

  @Test
  public void testUpdateFromStringWithVariables2() {
    AsciiGraphLoader asciiGraphLoader = AsciiGraphLoader.fromString("g[(a)-[e]->(b)]");

    validateCollections(asciiGraphLoader, 1, 2, 1);
    validateCaches(asciiGraphLoader, 1, 2, 1);

    asciiGraphLoader.appendFromString("g[(a)-[f]->(c)]");
    validateCollections(asciiGraphLoader, 1, 3, 2);
    validateCaches(asciiGraphLoader, 1, 3, 2);

    GradoopId g = asciiGraphLoader.getGraphIdByVariable("g");
    Vertex c = asciiGraphLoader.getVertexByVariable("c");
    Edge f = asciiGraphLoader.getEdgeByVariable("f");

    assertTrue(c.getGraphIds().contains(g), "Vertex not in graph");
    assertTrue(f.getGraphIds().contains(g), "Edge not in graph");
  }

  private void validateCollections(
      AsciiGraphLoader asciiGraphLoader,
      int expectedGraphIdCount,
      int expectedVertexCount,
      int expectedEdgeCount) {
    assertEquals(expectedGraphIdCount, asciiGraphLoader.getGraphIds().size(),
        "wrong number of graphs");
    assertEquals(expectedVertexCount, asciiGraphLoader.getVertices().size(),
        "wrong vertex count");
    assertEquals(expectedEdgeCount, asciiGraphLoader.getEdges().size(),
        "wrong edge count");
  }

  private void validateCaches(
      AsciiGraphLoader asciiGraphLoader,
      int expectedGraphIdCacheCount,
      int expectedVertexCacheCount,
      int expectedEdgeCacheCount) {
    assertEquals(expectedGraphIdCacheCount, asciiGraphLoader.getGraphIdCache().size(),
        "wrong graph head cache count");
    assertEquals(expectedVertexCacheCount, asciiGraphLoader.getVertexCache().size(),
        "wrong vertex cache count");
    assertEquals(expectedEdgeCacheCount, asciiGraphLoader.getEdgeCache().size(),
        "wrong edge cache count");
  }
}