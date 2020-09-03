package edu.leipzig.grafs.operators.union;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphOperatorI;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class UnionWithDuplicateInWindow<W extends Window> implements
    GraphToGraphOperatorI, GraphToGraphCollectionOperatorI {

  private final EdgeStream[] streams;
  private final WindowAssigner<Object, W> window;
  private final ProcessWindowFunction<EdgeContainer, EdgeContainer, String, W> FILTER_UNIQUE_IN_WINDOW_FUNCTION =
      new ProcessWindowFunction<EdgeContainer, EdgeContainer, String, W>() {
    @Override
    public void process(String s, Context context, Iterable<EdgeContainer> iterable,
        Collector<EdgeContainer> collector) throws Exception {
      collector.collect(iterable.iterator().next());
    }
  };

  public UnionWithDuplicateInWindow(WindowAssigner<Object, W> window, EdgeStream... streams) {

    this.streams = streams;
    this.window = window;
  }

  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    var unionedStream = new DisjunctUnion(streams).execute(stream);
    return unionedStream
        .keyBy(EdgeContainer::toString)
        .window(window)
        .process(FILTER_UNIQUE_IN_WINDOW_FUNCTION);

  }
}
