package streaming.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import org.gradoop.common.util.GradoopConstants;
import org.junit.jupiter.api.Test;
import streaming.model.Vertex;

class AsciiGraphLoaderTest {


  @Test
  public void testFromString() {
    AsciiGraphLoader asciiGraphLoader = AsciiGraphLoader.fromString("[()-->()]");

    validateCollections(asciiGraphLoader, 1, 2, 1);
  }

  @Test
  public void testFromFile() throws Exception {
    String file = URLDecoder.decode(
        getClass().getResource("/data/gdl/example.gdl").getFile(), StandardCharsets.UTF_8.name());
    AsciiGraphLoader asciiGraphLoader = AsciiGraphLoader.fromFile(file);

    validateCollections(asciiGraphLoader, 1, 2, 1);
  }

  @Test
  public void testGetVertices() {
    AsciiGraphLoader asciiGraphLoader = AsciiGraphLoader.fromString("[()]");

    validateCollections(asciiGraphLoader, 1, 1, 0);

    for (Vertex vertex : asciiGraphLoader.getVertices()) {
      assertEquals(GradoopConstants.DEFAULT_VERTEX_LABEL, vertex.getLabel(),
          "EPGMVertex has wrong label");
    }
  }

  private void validateCollections(
      AsciiGraphLoader asciiGraphLoader,
      int expectedGraphHeadCount,
      int expectedVertexCount,
      int expectedEdgeCount) {
    assertEquals(expectedGraphHeadCount, asciiGraphLoader.getGraphIds().size(),
        "wrong number of graphs");
    assertEquals(expectedVertexCount, asciiGraphLoader.getVertices().size(),
        "wrong vertex count");
    assertEquals(expectedEdgeCount, asciiGraphLoader.getEdges().size(),
        "wrong edge count");
  }
}