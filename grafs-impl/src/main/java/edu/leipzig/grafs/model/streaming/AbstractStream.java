package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.model.window.WindowsI;
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

  public <FW extends Window, W extends WindowsI<?>> S applyWindowedOperator(
      WindowedOperatorI<W> operatorI, WindowingInformation<?> wi) {
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

  public static class InitialWindowBuilder<S extends AbstractStream<S>, WBase extends WindowsI<? extends Window>> {

    private final S stream;
    private final WindowedOperatorI<WBase> operator;

    public InitialWindowBuilder(S stream,
        WindowedOperatorI<WBase> operator) {

      this.stream = stream;
      this.operator = operator;
    }

    public <WExtension extends WBase> WindowBuilder<S, WBase> withWindow(WExtension window) {
      return new WindowBuilder<>(stream, operator, window);
    }
  }

}
