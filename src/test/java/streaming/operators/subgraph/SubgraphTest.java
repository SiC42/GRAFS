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
