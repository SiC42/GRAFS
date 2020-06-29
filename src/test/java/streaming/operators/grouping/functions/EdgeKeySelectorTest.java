package streaming.operators.grouping.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import streaming.model.EdgeContainer;
import streaming.operators.grouping.model.GroupingInformation;
import streaming.util.AsciiGraphLoader;

class EdgeKeySelectorTest {

  @Test
  void getKey() {
    GroupingInformation egi = new GroupingInformation();
    egi.groupingKeys.add("n");
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(
        "(a18 {n : \"A\", a : \"18\"})," +
            "(a20 {n : \"A\", a : \"20\"})," +
            "(a25 {n : \"A\", a : \"25\"})," +
            "(b17 {n : \"B\", a : \"17\"})," +
            "(b19 {n : \"B\", a : \"19\"})," +
            "(c20 {n : \"C\", a : \"20\"})," +
            "(a18)-[]->(b17)," +
            "(a18)-[]->(c20)," +
            "(c20)-[]->(a25)," +
            "(c20)-[]->(b17)," +
            "(a20)-[]->(b19),"
    );
    Set<EdgeContainer> edgeSet = new HashSet<>(loader.getEdgeContainers());
    EdgeKeySelector eks = new EdgeKeySelector(egi, null, AggregateMode.SOURCE);
    for (EdgeContainer e : edgeSet) {
      assertThat(eks.getKey(e), equalTo("Vertex-Grouping-Information:(properties:{(n:A) })"));
    }
  }
}