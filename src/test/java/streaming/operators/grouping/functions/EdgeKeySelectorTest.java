package streaming.operators.grouping.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import streaming.helper.AsciiGraphLoader;
import streaming.model.Edge;
import streaming.operators.grouping.model.GroupingInformation;
import streaming.operators.grouping.functions.AggregateMode;
import streaming.operators.grouping.functions.EdgeKeySelector;

class EdgeKeySelectorTest {

  @Test
  void getKey() {
    GroupingInformation egi = new GroupingInformation();
    egi.groupingKeys.add("n");
    Set<Edge> edgeSet = new HashSet<>();
    edgeSet.addAll(AsciiGraphLoader.loadFromString(
        "(a18 {n : \"A\", a : \"18\"})," +
            "(a20 {n : \"A\", a : \"20\"})," +
            "(a25 {n : \"A\", a : \"25\"})," +
            "(b17 {n : \"B\", a : \"17\"})," +
            "(b19 {n : \"B\", a : \"19\"})," +
            "(c20 {n : \"C\", a : \"20\"})," +
            "(a18)-[]->(b17)," +
            "(a18)-[]->(c20)," +
            "(a20)-[]->(b19),"
    ));
    EdgeKeySelector eks = new EdgeKeySelector(egi, null, AggregateMode.SOURCE);
    for (Edge e : edgeSet) {
      Set<Edge> es = new HashSet<>();
      es.add(e);
      assertThat(eks.getKey(es), equalTo("Vertex-Grouping-Information:(properties:{(n:A) })"));
    }
  }
}