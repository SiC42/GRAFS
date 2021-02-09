package edu.leipzig.grafs.model.window;

import edu.leipzig.grafs.model.Triplet;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;

public class WindowingInformation<W extends Window> {

  WindowAssigner<? super Triplet<?, ?>, W> window;
  Trigger<? super Triplet<?, ?>, ? super W> trigger;
  Evictor<? super Triplet<?, ?>, ? super W> evictor;
  Time lateness;
  OutputTag<Triplet<?, ?>> outputTag;

  public WindowingInformation(WindowAssigner<? super Triplet<?, ?>, W> window) {
    this.window = window;
    trigger = null;
    evictor = null;
    lateness = null;
    outputTag = null;
  }

  public void addTrigger(Trigger<? super Triplet<?, ?>, ? super W> trigger) {
    this.trigger = trigger;
  }

  public void addEvictor(Evictor<? super Triplet<?, ?>, ? super W> evictor) {
    this.evictor = evictor;
  }

  public void addAllowedLateness(Time lateness) {
    this.lateness = lateness;
  }

  public void addLateDataOutputTag(OutputTag<Triplet<?, ?>> outputTag) {
    this.outputTag = outputTag;
  }

  public WindowAssigner<? super Triplet<?, ?>, W> getWindow() {
    return window;
  }

  public Trigger<? super Triplet<?, ?>, ? super W> getTrigger() {
    return trigger;
  }

  public Evictor<? super Triplet<?, ?>, ? super W> getEvictor() {
    return evictor;
  }

  public Time getLateness() {
    return lateness;
  }

  public OutputTag<Triplet<?, ?>> getOutputTag() {
    return outputTag;
  }
}
