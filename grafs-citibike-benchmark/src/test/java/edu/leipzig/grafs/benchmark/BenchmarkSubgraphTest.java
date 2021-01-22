package edu.leipzig.grafs.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.leipzig.grafs.benchmark.operators.subgraph.BenchmarkSubgraph;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.GraphStream;
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

    GraphStream input = loader.createEdgeStream(getConfig());

    Collection<Triplet> expected =
        loader.createTripletsByGraphVariables("expected");

    Iterator<Triplet> output = input
        .callForGraph(new BenchmarkSubgraph(
            v -> v.getLabel().equals("Person"),
            e -> e.getLabel().equals("knows"),
            Strategy.BOTH))
        .collect();
    Collection<Triplet> actual = new HashSet<>();
    output.forEachRemaining(actual::add);
    assertEquals(expected, actual);
  }

}