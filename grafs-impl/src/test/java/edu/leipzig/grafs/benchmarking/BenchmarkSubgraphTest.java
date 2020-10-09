package edu.leipzig.grafs.benchmarking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.leipzig.grafs.benchmarking.subgraph.BenchmarkSubgraph;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.OperatorTestBase;
import edu.leipzig.grafs.operators.subgraph.Subgraph.Strategy;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import org.junit.jupiter.api.Test;

public class BenchmarkSubgraphTest extends OperatorTestBase {

  @Test
  public void test() throws Exception {
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
        .callForStream(new BenchmarkSubgraph(
            v -> v.getLabel().equals("Person"),
            e -> e.getLabel().equals("knows"),
            Strategy.BOTH))
        .collect();
    Collection<EdgeContainer> actual = new HashSet<>();
    output.forEachRemaining(actual::add);
    assertEquals(expected, actual);
  }

}