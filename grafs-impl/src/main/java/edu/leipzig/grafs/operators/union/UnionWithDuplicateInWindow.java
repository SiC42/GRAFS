package edu.leipzig.grafs.operators.union;

import com.google.common.annotations.Beta;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * This operator unifies multiple streams into the given one. Duplicate elements are detected in a
 * window and only one is returned. It is advised to only use Tumbling Windows.
 */
@Beta
public class UnionWithDuplicateInWindow<W extends Window> implements
    GraphToGraphOperatorI, GraphToGraphCollectionOperatorI {

  /**
   * Streams that should be unified via the union operation.
   */
  private final EdgeStream[] streams;

  /**
   * Window in which only distinct elements should exist
   */
  private final WindowAssigner<Object, W> window;

  /**
   * Optional trigger
   */
  private final Trigger<Triplet, W> trigger;

  /**
   * Initializes the operator with the given parameters.
   *
   * @param window  window in which only distinct elements should exist
   * @param streams streams that should be unified by applying the union operator onto a stream
   */
  public UnionWithDuplicateInWindow(WindowAssigner<Object, W> window, EdgeStream... streams) {
    this(window, null, streams);
  }

  /**
   * Initializes the operator with the given parameters.
   *
   * @param window  window in which only distinct elements should exist
   * @param streams streams that should be unified by applying the union operator onto a stream
   * @param trigger optional window trigger that is used for this operation
   */
  public UnionWithDuplicateInWindow(WindowAssigner<Object, W> window,
      Trigger<Triplet, W> trigger, EdgeStream... streams) {
    this.streams = streams;
    this.trigger = trigger;
    this.window = window;
  }

  /**
   * Applies the streams to this stream and returns the unified stream with distinct elements in the
   * given window.
   *
   * @param stream stream on which the operator should be applied
   * @return unified stream
   */
  @Override
  public DataStream<Triplet> execute(DataStream<Triplet> stream) {
    var unionedStream = new DisjunctUnion(streams).execute(stream);
    var filterDuplicateInWindowFunction =
        new ProcessWindowFunction<Triplet, Triplet, String, W>() {
          @Override
          public void process(String s, Context context, Iterable<Triplet> iterable,
              Collector<Triplet> collector) throws Exception {
            collector.collect(iterable.iterator().next());
          }
        };
    return unionedStream
        .keyBy(triplet -> triplet.getEdge().getId().toString())
        .window(window)
        .trigger(trigger)
        .process(filterDuplicateInWindowFunction);

  }
}
