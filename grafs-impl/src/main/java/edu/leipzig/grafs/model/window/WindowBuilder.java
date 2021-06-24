package edu.leipzig.grafs.model.window;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GCStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.operators.interfaces.window.WindowedOperatorI;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class WindowBuilder<S extends AbstractStream<S>, W extends Window> {


  private final S stream;
  private final WindowingInformation<?> wi;

  public WindowBuilder(S stream, WindowAssigner<Object, W> window) {
    this.stream = stream;
    wi = new WindowingInformation<>(window);
  }

  public WindowBuilder<S, W> trigger(Trigger<? super Triplet<?, ?>, ? super Window> trigger) {
    wi.addTrigger(trigger);
    return this;
  }

  public WindowBuilder<S, W> evictor(Evictor<? super Triplet<?, ?>, ? super Window> evictor) {
    wi.addEvictor(evictor);
    return this;
  }

  public WindowBuilder<S, W> allowedLateness(Time lateness) {
    wi.addAllowedLateness(lateness);
    return this;
  }

  public GraphStream callForGraph(WindowedOperatorI<S,GraphStream> operator) {
    DataStream<Triplet<Vertex, Edge>> result = operator.execute(stream.getDataStream(), wi);
    return new GraphStream(result, stream.getConfig());
  }

  public GCStream callForGC(WindowedOperatorI<S,GCStream> operator) {
    var result = operator.execute(stream.getDataStream(), wi);
    return new GCStream(result, stream.getConfig());
  }
}
