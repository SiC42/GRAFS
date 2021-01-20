package edu.leipzig.grafs.operators.union;

import com.google.common.annotations.Beta;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.model.streaming.AbstractWindowedStream.WindowInformation;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowGraphCollectionToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.windowed.WindowGraphToGraphOperatorI;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * This operator unifies multiple streams into the given one. Duplicate elements are detected in a
 * window and only one is returned. It is advised to only use Tumbling Windows.
 */
@Beta
public class UnionWithDuplicateInWindow implements
    WindowGraphToGraphOperatorI, WindowGraphCollectionToGraphCollectionOperatorI {

  /**
   * Streams that should be unified via the union operation.
   */
  private final GraphStream[] streams;

  /**
   * Initializes the operator with the given parameters.
   *
   * @param streams streams that should be unified by applying the union operator onto a stream
   */
  public UnionWithDuplicateInWindow(GraphStream... streams) {
    this.streams = streams;
  }

  /**
   * Applies the streams to this stream and returns the unified stream with distinct elements in the
   * given window.
   *
   * @param stream stream on which the operator should be applied
   * @return unified stream
   */
  @Override
  public <W extends Window> DataStream<Triplet> execute(DataStream<Triplet> stream,
      WindowInformation<W> wi) {
    var unionedStream = new DisjunctUnion(streams).execute(stream);
    var filterDuplicateInWindowFunction =
        new ProcessWindowFunction<Triplet, Triplet, String, W>() {
          @Override
          public void process(String s, Context context, Iterable<Triplet> iterable,
              Collector<Triplet> collector) throws Exception {
            collector.collect(iterable.iterator().next());
          }
        };
    var unionedWindowStream = unionedStream
        .keyBy(triplet -> triplet.getEdge().getId().toString())
        .window(wi.getWindow());
    unionedWindowStream = applyOtherWindowInformation(unionedWindowStream, wi);
    return unionedWindowStream.process(filterDuplicateInWindowFunction);

  }
}
