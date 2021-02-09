package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.window.WindowingInformation;
import edu.leipzig.grafs.model.window.WindowsI;
import edu.leipzig.grafs.operators.interfaces.window.WindowedOperatorI;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;

public class WindowBuilder<S extends AbstractStream<S>, WBase extends WindowsI<?>> {


  private final S stream;
  private final WindowedOperatorI<?> operator;
  private final WindowingInformation<?> wi;

  public <W extends WBase> WindowBuilder(S stream, WindowedOperatorI<WBase> operator, W window) {
    this.stream = stream;
    this.operator = operator;
    wi = new WindowingInformation<>(window.getFlinkWindowAssigner());
  }

  public WindowBuilder<S, WBase> trigger(Trigger<? super Triplet<?, ?>, ? super Window> trigger) {
    wi.addTrigger(trigger);
    return this;
  }

  public WindowBuilder<S, WBase> evictor(Evictor<? super Triplet<?, ?>, ? super Window> evictor) {
    wi.addEvictor(evictor);
    return this;
  }

  public WindowBuilder<S, WBase> allowedLateness(Time lateness) {
    wi.addAllowedLateness(lateness);
    return this;
  }

  public WindowBuilder<S, WBase> sideOutputLateData(OutputTag<Triplet<?, ?>> outputTag) {
    wi.addLateDataOutputTag(outputTag);
    return this;
  }

  public S apply() {
    return stream.applyWindowedOperator(operator, wi);
  }
}
