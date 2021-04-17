package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.nonwindow.GraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedGraphCollectionToGraphOperatorI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedOperatorI;
import edu.leipzig.grafs.util.FlinkConfig;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

public abstract class AbstractStream<S extends AbstractStream<?>> {


  protected final FlinkConfig config;
  protected DataStream<Triplet<Vertex, Edge>> stream;

  /**
   * Constructs an triplet stream with the given data stream and config.
   *
   * @param stream data stream that holds <tt>Triplet</tt>
   * @param config config used for the stream
   */
  public AbstractStream(DataStream<Triplet<Vertex, Edge>> stream, FlinkConfig config) {
    this.stream = stream.assignTimestampsAndWatermarks(config.getWatermarkStrategy());
    this.config = config;
  }

  protected static DataStream<Triplet<Vertex, Edge>> prepareStream(
      SourceFunction<Triplet<Vertex, Edge>> function,
      FlinkConfig config, String sourceName) {
    return config.getExecutionEnvironment().addSource(function, sourceName, TypeInformation
        .of(new TypeHint<>() {
        }));
  }

  /**
   * Returns the underlying data stream.
   *
   * @return the underlying data stream
   */
  public DataStream<Triplet<Vertex, Edge>> getDataStream() {
    return stream;
  }

  public S applyWindowedOperator(
      WindowedOperatorI<?,?> operatorI, WindowingInformation<?> wi) {
    stream = operatorI.execute(stream, wi);
    return getThis();
  }

  protected abstract S getThis();

  /**
   * Adds a sink function to the stream, which determines what should happen with the stream at the
   * end.
   * <p>
   * Only works once!
   *
   * @param sinkFunction The object containing the sink's invoke function.
   */
  public void addSink(SinkFunction<Triplet<Vertex, Edge>> sinkFunction) {
    stream.addSink(sinkFunction);
  }

  /**
   * Prints the stream to stdout.
   */
  public void print() {
    stream.print();
  }

  /**
   * Collects the stream into a iterator
   *
   * @return iterator of the stream content
   * @throws IOException
   */
  public Iterator<Triplet<Vertex, Edge>> collect() throws IOException {
    return DataStreamUtils.collect(stream);
  }

  public static class InitialWindowBuilder<S extends AbstractStream<S>> {

    private final S stream;
    private final WindowedOperatorI<?,?> operator;

    public InitialWindowBuilder(S stream,
        WindowedOperatorI<?,?> operator) {

      this.stream = stream;
      this.operator = operator;
    }

    public <W extends Window> WindowBuilder<S, W> withWindow(
        WindowAssigner<Object, W> window) {
      stream.getDataStream().windowAll(window);
      stream.getDataStream().windowAll(TumblingEventTimeWindows.of(Time.minutes(5)));
      return new WindowBuilder<>(stream, operator, window);
    }
  }

  public GraphStream callForGraph(GraphCollectionToGraphOperatorI operator) {
    DataStream<Triplet<Vertex, Edge>> result = operator.execute(stream);
    return new GraphStream(result, config);
  }

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
