package streaming.operators;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import streaming.helper.AsciiGraphLoader;
import streaming.model.Edge;
import streaming.model.grouping.PropertiesAggregationFunction;
import streaming.model.grouping.AggregationMapping;
import streaming.model.grouping.ElementGroupingInformation;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;

class VertexAggregationFunctionTest {

    @Test
    void flatMap() {
        ElementGroupingInformation egi = new ElementGroupingInformation();
        egi.groupingKeys.add("n");
        AggregationMapping am = new AggregationMapping();
        am.addAggregation("a", new PropertiesAggregationFunction("0", (String pV1, String pV2) -> String.valueOf(Double.parseDouble(pV1) + Double.parseDouble(pV2))));

        VertexAggregationFunction incrementer = new VertexAggregationFunction(egi, am, AggregateMode.SOURCE);

        Collector<Edge> collector = mock(Collector.class);

        Collection<Edge> edges = AsciiGraphLoader.loadFromString(
                "(a1 {n : \"A\", a : \"1\"})," +
                        "(a2 {n : \"A\", a : \"2\"})," +
                        "(b1 {n : \"A\", a : \"25\"})," +
                        "(b2 {n : \"B\", a : \"17\"})," +
                        "(b19 {n : \"B\", a : \"19\"})," +
                        "(c20 {n : \"C\", a : \"20\"})," +
                        "(a18)-[]->(b18)," +
                        "(a18)-[]->(c20)," +
                        "(c20)-[]->(a25)," +
                        "(c20)-[]->(b17)," +
                        "(a20)-[]->(b19),"
        );
        Set<Edge> edgeSet = new HashSet<>();
        edgeSet.addAll(edges);


        // call the methods that you have implemented
        //incrementer.flatMap(2L, collector);

        //verify collector was called with the right output
        //Mockito.verify(collector, times(1)).collect(3L);
    }
}