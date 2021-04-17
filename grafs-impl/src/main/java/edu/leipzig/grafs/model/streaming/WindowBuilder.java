package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.operators.interfaces.window.WindowedOperatorI;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class WindowBuilder<S extends AbstractStream<S>, W extends Window> {


  private final S stream;
  private final WindowedOperatorI operator;
  private final WindowingInformation<?> wi;

  public WindowBuilder(S stream, WindowedOperatorI operator, WindowAssigner<Object, W> window) {
    this.stream = stream;
    this.operator = operator;
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

  public S apply() {
    return stream.applyWindowedOperator(operator, wi);
  }
}
