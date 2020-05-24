package streaming.model;

import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import streaming.operators.OperatorI;
import streaming.operators.grouping.Grouping;
import streaming.operators.grouping.model.AggregationMapping;
import streaming.operators.grouping.model.GroupingInformation;
import streaming.operators.subgraph.Subgraph;
import streaming.operators.subgraph.Subgraph.Strategy;
import streaming.operators.transform.EdgeTransformation;
import streaming.operators.transform.VertexTransformation;

public class EdgeStream {

  private DataStream<Edge> edgeStream;

  public EdgeStream(DataStream<Edge> edgeStream) {
    this.edgeStream = edgeStream.assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<Edge>() {
          @Override
          public long extractAscendingTimestamp(Edge edge) { // TODO: timestamp define
            return 0;
          }
        }
    );
  }

  public EdgeStream callForGraph(OperatorI operator) {
    return operator.execute(edgeStream);
  }

  public EdgeStream vertexInducedSubgraph(
      FilterFunction<GraphElementInformation> vertexGeiPredicate) {
    return callForGraph(new Subgraph(vertexGeiPredicate, null, Strategy.VERTEX_INDUCED));
  }

  public EdgeStream edgeInducedSubgraph(FilterFunction<GraphElementInformation> edgeGeiPredicate) {
    return callForGraph(new Subgraph(null, edgeGeiPredicate, Strategy.EDGE_INDUCED));
  }

  public EdgeStream subgraph(FilterFunction<GraphElementInformation> vertexGeiPredicate,
      FilterFunction<GraphElementInformation> edgeGeiPredicate) {
    return callForGraph(new Subgraph(vertexGeiPredicate, edgeGeiPredicate, Strategy.BOTH));
  }

  public EdgeStream groupBy(GroupingInformation vertexEgi,
      AggregationMapping vertexAggregationFunctions,
      GroupingInformation edgeEgi, AggregationMapping edgeAggregationFunctions) {
    return callForGraph(
        new Grouping(vertexEgi, vertexAggregationFunctions, edgeEgi, edgeAggregationFunctions));
  }

  public EdgeStream transformEdge(MapFunction<Edge, Edge> mapper) {
    return callForGraph(new EdgeTransformation(mapper));
  }

  public EdgeStream transformVertices(MapFunction<Vertex, Vertex> mapper) {
    return callForGraph(new VertexTransformation(mapper));
  }

  public void print() {
    edgeStream.print();
  }

  public Iterator<Edge> collect() throws IOException {
    return DataStreamUtils.collect(edgeStream);
  }
}