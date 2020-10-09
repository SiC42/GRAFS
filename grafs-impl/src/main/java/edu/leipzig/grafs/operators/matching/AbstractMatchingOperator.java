package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.matching.logic.EdgeQueryFilter;
import edu.leipzig.grafs.operators.matching.logic.VertexQueryFilter;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public abstract class AbstractMatchingOperator<W extends Window> implements
    GraphToGraphCollectionOperatorI {

  final QueryGraph queryGraph;
  final WindowAssigner<Object, W> window;
  final Trigger<EdgeContainer, W> trigger;


  public AbstractMatchingOperator(String query, WindowAssigner<Object, W> window,
      Trigger<EdgeContainer, W> trigger) {
    var graph = AsciiGraphLoader.fromString(query).createGraph();
    this.queryGraph = QueryGraph.fromGraph(graph);
    this.window = window;
    this.trigger = trigger;
  }

  AllWindowedStream<EdgeContainer, W> preProcessAndApplyWindow(DataStream<EdgeContainer> stream) {
    var filteredStream = queryGraph.isVertexOnly() ?
        stream.filter(new VertexQueryFilter(queryGraph)) :
        stream.filter(new EdgeQueryFilter(queryGraph));
    var windowedStream = filteredStream.windowAll(window);
    if (trigger != null) {
      windowedStream = windowedStream.trigger(trigger);
    }
    return windowedStream;
  }

}
