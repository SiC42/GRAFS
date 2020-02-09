package streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class EdgeStream {


    private DataStream<Edge> edgeStream;

    public EdgeStream(DataStream<Edge> edgeStream) {
        this.edgeStream = edgeStream;
    }

    public EdgeStream filter(FilterFunction<Edge> predicate) {
        DataStream<Edge> filteredStream = edgeStream.filter(predicate);
        return new EdgeStream(filteredStream);
    }

    public EdgeStream transform(MapFunction<Edge, Edge> mapper) {
        DataStream<Edge> filteredStream = edgeStream.map(mapper);
        return new EdgeStream(filteredStream);
    }

    public EdgeStream transformVertices(UnaryOperator<String> mapper) {
        MapFunction<Edge, Edge> transformVerticesFunction =
                edge -> {
                    String from = mapper.apply(edge.getFrom());
                    String to = mapper.apply(edge.getTo());
                    return new Edge(from, to, edge.getContent());
                };
        return transform(transformVerticesFunction);
    }

    public void print() {
        edgeStream.print();
    }
}