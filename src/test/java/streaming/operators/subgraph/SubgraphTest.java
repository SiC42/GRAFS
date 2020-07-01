package streaming.operators.subgraph;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.junit.jupiter.api.Test;
import streaming.model.EdgeContainer;
import streaming.model.EdgeStream;
import streaming.operators.OperatorTestBase;
import streaming.util.AsciiGraphLoader;

public class SubgraphTest extends OperatorTestBase {

  @Test
  public void testExistingSubgraph() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendFromString("expected:_DB[" +
        "(alice)-[akb]->(bob)-[bkc]->(carol)-[ckd]->(dave)" +
        "(alice)<-[bka]-(bob)<-[ckb]-(carol)<-[dkc]-(dave)" +
        "(eve)-[eka]->(alice)" +
        "(eve)-[ekb]->(bob)" +
        "(frank)-[fkc]->(carol)" +
        "(frank)-[fkd]->(dave)" +
        "]");

    EdgeStream input = loader.createEdgeStream(getConfig());

    Collection<EdgeContainer> expected =
        loader.createEdgeContainersByGraphVariables("expected");

    Iterator<EdgeContainer> output = input
        .subgraph(
            v -> v.getLabel().equals("Person"),
            e -> e.getLabel().equals("knows"))
        .collect();
    Collection<EdgeContainer> actual = new HashSet<>();
    while (output.hasNext()) {
      actual.add(output.next());
    }
    assertEquals(expected, actual);
  }

  /**
   * Extracts a subgraph where only vertices fulfill the filter function.
   */
  @Test
  public void testPartialSubgraph() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendFromString("expected:_DB[" +
        "(alice),(bob),(carol),(dave),(eve),(frank)" +
        "]");

    EdgeStream input = loader.createEdgeStream(getConfig());

    Collection<EdgeContainer> expected =
        loader.createEdgeContainersByGraphVariables("expected");

    Iterator<EdgeContainer> output = input
        .subgraph(
            v -> v.getLabel().equals("Person"),
            e -> e.getLabel().equals("friendOf")).collect();

    Collection<EdgeContainer> actual = new HashSet<>();
    while (output.hasNext()) {
      actual.add(output.next());
    }
    assertEquals(expected, actual);
  }

  /**
   * Extracts a subgraph which is empty.
   *
   * @throws Exception on failure
   */
  @Test
  public void testEmptySubgraph() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendFromString("expected:_DB[]");

    EdgeStream input = loader.createEdgeStream(getConfig());

    Collection<EdgeContainer> expected =
        loader.createEdgeContainersByGraphVariables("expected");

    Iterator<EdgeContainer> output = input.subgraph(
        v -> v.getLabel().equals("User"),
        e -> e.getLabel().equals("friendOf")).collect();

    Collection<EdgeContainer> actual = new HashSet<>();
    while (output.hasNext()) {
      actual.add(output.next());
    }
    assertEquals(expected, actual);
  }

  @Test
  public void testVertexInducedSubgraph() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendFromString("expected:_DB[" +
        "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
        "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
        "]");

    EdgeStream input = loader.createEdgeStream(getConfig());

    Collection<EdgeContainer> expected =
        loader.createEdgeContainersByGraphVariables("expected");

    Iterator<EdgeContainer> output = input.vertexInducedSubgraph(
        v -> v.getLabel().equals("Forum") || v.getLabel().equals("Tag")).collect();

    Collection<EdgeContainer> actual = new HashSet<>();
    while (output.hasNext()) {
      actual.add(output.next());
    }
    assertEquals(expected, actual);
  }

  @Test
  public void testEdgeInducedSubgraph() throws Exception {
    AsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendFromString("expected:_DB[" +
        "(databases)<-[ghtd]-(gdbs)-[ghtg1]->(graphs)" +
        "(graphs)<-[ghtg2]-(gps)-[ghth]->(hadoop)" +
        "]");

    EdgeStream input = loader.createEdgeStream(getConfig());

    Collection<EdgeContainer> expected =
        loader.createEdgeContainersByGraphVariables("expected");

    Iterator<EdgeContainer> output = input.edgeInducedSubgraph(
        e -> e.getLabel().equals("hasTag")).collect();
    Collection<EdgeContainer> actual = new HashSet<>();
    while (output.hasNext()) {
      actual.add(output.next());
    }
    assertEquals(expected, actual);
  }


}