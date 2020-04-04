package streaming.operators;

import org.junit.jupiter.api.Test;
import streaming.helper.AsciiGraphLoader;
import streaming.model.Edge;
import streaming.model.grouping.ElementGroupingInformation;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

class EdgeKeySelectorTest {

    @Test
    void getKey() {
        ElementGroupingInformation egi = new ElementGroupingInformation();
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
        EdgeKeySelector eks = new EdgeKeySelector(egi,null, true);
        for(Edge e : edgeSet){
            Set<Edge> es = new HashSet<>();
            es.add(e);
            assertThat(eks.getKey(es), equalTo("Vertex-Grouping-Information:(properties:{(n:A) })"));
        }
    }
}