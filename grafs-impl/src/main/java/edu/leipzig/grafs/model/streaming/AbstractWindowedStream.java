package edu.leipzig.grafs.model.streaming;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.util.FlinkConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;

public abstract class AbstractWindowedStream<W extends Window,WS extends AbstractWindowedStream<W,?>> extends AbstractStream {



  protected final WindowInformation<W> wi;

  public AbstractWindowedStream(DataStream<Triplet> stream, FlinkConfig config,
      WindowAssigner<? super Triplet, W> window) {
    super(stream, config);
    this.wi = new WindowInformation<>(window);
  }

  protected abstract WS getThis();

  public WS trigger(Trigger<? super Triplet, ? super W> trigger) {
    wi.addTrigger(trigger);
    return getThis();
  }

  public WS evictor(Evictor<? super Triplet, ? super W> evictor) {
    wi.addEvictor(evictor);
    return getThis();
  }

  public WS allowedLateness(Time lateness) {
    wi.addAllowedLateness(lateness);
    return getThis();
  }

  public WS sideOutputLateData(OutputTag<Triplet> outputTag) {
    wi.addLateDataOutputTag(outputTag);
    return getThis();
  }


  public static class WindowInformation<W extends Window> {

    private final WindowAssigner<? super Triplet, W> window;
    private Trigger<? super Triplet, ? super W> trigger;
    private Evictor<? super Triplet, ? super W> evictor;
    private Time lateness;
    private OutputTag<Triplet> outputTag;

    public WindowInformation(WindowAssigner<? super Triplet, W> window) {
      this.window = window;
      trigger = null;
      evictor = null;
      lateness = null;
      outputTag = null;
    }

    public void addTrigger(Trigger<? super Triplet, ? super W> trigger) {
      this.trigger = trigger;
    }

    public void addEvictor(Evictor<? super Triplet, ? super W> evictor) {
      this.evictor = evictor;
    }

    public void addAllowedLateness(Time lateness) {
      this.lateness = lateness;
    }

    public void addLateDataOutputTag(OutputTag<Triplet> outputTag) {
      this.outputTag = outputTag;
    }

    public WindowAssigner<? super Triplet, W> getWindow() {
      return window;
    }

    public Trigger<? super Triplet, ? super W> getTrigger() {
      return trigger;
    }

    public Evictor<? super Triplet, ? super W> getEvictor() {
      return evictor;
    }

    public Time getLateness() {
      return lateness;
    }

    public OutputTag<Triplet> getOutputTag() {
      return outputTag;
    }
  }

}
