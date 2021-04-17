package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphToGraphOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphToGraphOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Model that abstracts the data stream to a edge(container)-stream.
 */
public class GraphStream extends AbstractStream<GraphStream> implements GraphStreamOperators {

  /**
   * Constructs an graph stream with the given data stream and config.
   *
   * @param stream data stream that holds <tt>Triplet</tt>
   * @param config config used for the stream
   */
  public GraphStream(DataStream<Triplet<Vertex, Edge>> stream, FlinkConfig config) {
    super(stream, config);
  }


  public static GraphStream fromSource(SourceFunction<Triplet<Vertex, Edge>> function,
      FlinkConfig config) {
    return fromSource(function, config, "Custom Source");
  }

  public static GraphStream fromSource(SourceFunction<Triplet<Vertex, Edge>> function,
      FlinkConfig config,
      String sourceName) {
    var tripletStream = prepareStream(function, config, sourceName);
    return new GraphStream(tripletStream, config);
  }

  @Override
  protected GraphStream getThis() {
    return this;
  }

  /**
   * Creates an triplet stream from this stream using the given operator.
   *
   * @param operator operator that should be used on this stream
   * @return result of given operator
   */
  public GraphStream callForGraph(GraphToGraphOperatorI operator) {
    DataStream<Triplet<Vertex, Edge>> result = operator.execute(stream);
    return new GraphStream(result, config);
  }

  @Override
  public GCStream callForGC(GraphToGraphCollectionOperatorI operator) {
    DataStream<Triplet<Vertex, Edge>> result = operator.execute(stream);
    return new GCStream(result, config);
  }

  public InitialWindowBuilder<GraphStream> callForGraph(
      WindowedGraphToGraphOperatorI operator) {
    return new InitialWindowBuilder<>(new GraphStream(stream, config), operator);
  }

  public InitialWindowBuilder<GCStream> callForGC(
      WindowedGraphToGraphCollectionOperatorI operator) {
    return new InitialWindowBuilder<>(new GCStream(stream, config), operator);
  }
}