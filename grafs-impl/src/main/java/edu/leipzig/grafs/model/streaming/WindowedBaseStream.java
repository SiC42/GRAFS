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

public abstract class WindowedBaseStream<W extends Window> {

  protected final DataStream<Triplet> gcStream;
  protected final FlinkConfig config;
  protected final WindowInformation<W> wi;

  public WindowedBaseStream(DataStream<Triplet> gcStream, FlinkConfig config,
      WindowAssigner<Object, W> window) {
    this.gcStream = gcStream.assignTimestampsAndWatermarks(config.getWatermarkStrategy());
    this.config = config;
    this.wi = new WindowInformation<>(window);
  }

  public <WS extends WindowedBaseStream<W>> WS trigger(Trigger<Triplet, W> trigger){
    wi.addTrigger(trigger);
    return (WS)this;
  }

  public <WS extends WindowedBaseStream<W>> WS evictor(Evictor<Triplet, W> evictor){
    wi.addEvictor(evictor);
    return (WS)this;
  }

  public <WS extends WindowedBaseStream<W>> WS allowedLateness(Time lateness){
    wi.addAllowedLateness(lateness);
    return (WS)this;
  }

  public <WS extends WindowedBaseStream<W>> WS sideOutputLateData(OutputTag<Triplet> outputTag){
    wi.addLateDataOutputTag(outputTag);
    return (WS)this;
  }


  public static class WindowInformation<W extends Window>{

    private final WindowAssigner<Object, W> window;
    private Trigger<Triplet, W> trigger;
    private Evictor<Triplet, W> evictor;
    private Time lateness;
    private OutputTag<Triplet> outputTag;

    public WindowInformation(WindowAssigner<Object, W> window) {
      this.window = window;
      trigger = null;
      evictor = null;
      lateness = null;
      outputTag = null;
    }

    public void addTrigger(Trigger<Triplet, W> trigger){
      this.trigger = trigger;
    }

    public void addEvictor(Evictor<Triplet, W> evictor){
      this.evictor = evictor;
    }

    public void addAllowedLateness(Time lateness){
      this.lateness = lateness;
    }

    public void addLateDataOutputTag(OutputTag<Triplet> outputTag){
      this.outputTag = outputTag;
    }

    public WindowAssigner<Object, W> getWindow() {
      return window;
    }

    public Trigger<Triplet, W> getTrigger() {
      return trigger;
    }

    public Evictor<Triplet, W> getEvictor() {
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
