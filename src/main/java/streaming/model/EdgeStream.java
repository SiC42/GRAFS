package streaming.model;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

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

    public EdgeStream transformVertices(MapFunction<Vertex,Vertex> mapper) {
        MapFunction<Edge, Edge> transformVerticesFunction =
                edge -> {
                    Vertex from = mapper.map(edge.getSource());
                    Vertex to = mapper.map(edge.getTarget());
                    return new Edge(from, to, edge.getLabel(), edge.getProperties(), edge.getMemberships());
                };
        return transform(transformVerticesFunction);
    }

    public void print() {
        edgeStream.print();
    }
}