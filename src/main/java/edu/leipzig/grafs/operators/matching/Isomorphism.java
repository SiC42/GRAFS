package edu.leipzig.grafs.operators.matching;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.operators.interfaces.GraphToGraphCollectionOperatorI;
import edu.leipzig.grafs.operators.matching.logic.EdgeQueryFilter;
import edu.leipzig.grafs.operators.matching.logic.IsomorphismMatchingProcess;
import edu.leipzig.grafs.operators.matching.logic.VertexQueryFilter;
import edu.leipzig.grafs.operators.matching.model.QueryGraph;
import edu.leipzig.grafs.util.AsciiGraphLoader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class Isomorphism<W extends Window> implements GraphToGraphCollectionOperatorI {

  private final WindowAssigner<Object, W> window;
  private final QueryGraph queryGraph;

  public Isomorphism(String query, WindowAssigner<Object, W> window) {
    var graph = AsciiGraphLoader.fromString(query).createGraph();
    this.queryGraph = QueryGraph.fromGraph(graph);
    this.window = window;
  }


  @Override
  public DataStream<EdgeContainer> execute(DataStream<EdgeContainer> stream) {
    var filteredStream = queryGraph.isVertexOnly() ?
        stream.filter(new VertexQueryFilter(queryGraph)) :
        stream.filter(new EdgeQueryFilter(queryGraph));
    return filteredStream.windowAll(window).process(new IsomorphismMatchingProcess<>(queryGraph));
  }
}
