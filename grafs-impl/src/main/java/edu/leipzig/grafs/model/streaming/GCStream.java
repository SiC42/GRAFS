package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.window.WindowsI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class GCStream extends AbstractStream<GCStream> implements GCStreamOperators {

  /**
   * Constructs a graph stream with the given data stream and config.
   *
   * @param stream data stream that holds <tt>Triplet</tt>
   * @param config config used for the stream
   */
  public GCStream(
      DataStream<Triplet<Vertex, Edge>> stream, FlinkConfig config) {
    super(stream, config);
  }

  public static GCStream fromSource(SourceFunction<Triplet<Vertex, Edge>> function,
      FlinkConfig config) {
    return fromSource(function, config, "Custom Source");
  }

  public static GCStream fromSource(SourceFunction<Triplet<Vertex, Edge>> function,
      FlinkConfig config,
      String sourceName) {
    var tripletStream = prepareStream(function, config, sourceName);
    return new GCStream(tripletStream, config);
  }

  @Override
  protected GCStream getThis() {
    return this;
  }

  @Override
  public GraphStream callForGraph(GraphCollectionToGraphOperatorI operator) {
    DataStream<Triplet<Vertex, Edge>> result = operator.execute(stream);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(GraphCollectionToGraphCollectionOperatorI operator) {
    var result = operator.execute(stream);
    return new GCStream(result, config);
  }

  public InitialWindowBuilder<GraphStream> callForGraph(
      WindowedGraphCollectionToGraphOperatorI operator) {
    return new InitialWindowBuilder<>(new GraphStream(stream, config), operator);
  }

  //@Override
  public InitialWindowBuilder<GCStream> callForGC(
      WindowedGraphCollectionToGraphCollectionOperatorI operator) {
    return new InitialWindowBuilder<>(new GCStream(stream, config), operator);
  }

}
