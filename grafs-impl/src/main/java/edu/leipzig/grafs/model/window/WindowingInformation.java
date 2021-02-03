package edu.leipzig.grafs.model.window;

import edu.leipzig.grafs.model.BasicTriplet;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.OutputTag;

public class WindowingInformation<W extends Window> {

  WindowAssigner<? super BasicTriplet<?,?>, W> window;
  Trigger<? super BasicTriplet<?,?>, ? super W> trigger;
  Evictor<? super BasicTriplet<?,?>, ? super W> evictor;
  Time lateness;
  OutputTag<BasicTriplet<?,?>> outputTag;

  public WindowingInformation(WindowAssigner<? super BasicTriplet<?,?>, W> window) {
    this.window = window;
    trigger = null;
    evictor = null;
    lateness = null;
    outputTag = null;
  }

  public void addTrigger(Trigger<? super BasicTriplet<?,?>, ? super W> trigger) {
    this.trigger = trigger;
  }

  public void addEvictor(Evictor<? super BasicTriplet<?,?>, ? super W> evictor) {
    this.evictor = evictor;
  }

  public void addAllowedLateness(Time lateness) {
    this.lateness = lateness;
  }

  public void addLateDataOutputTag(OutputTag<BasicTriplet<?,?>> outputTag) {
    this.outputTag = outputTag;
  }

  public WindowAssigner<? super BasicTriplet<?,?>, W> getWindow() {
    return window;
  }

  public Trigger<? super BasicTriplet<?,?>, ? super W> getTrigger() {
    return trigger;
  }

  public Evictor<? super BasicTriplet<?,?>, ? super W> getEvictor() {
    return evictor;
  }

  public Time getLateness() {
    return lateness;
  }

  public OutputTag<BasicTriplet<?,?>> getOutputTag() {
    return outputTag;
  }
}
