package edu.leipzig.grafs.model.window;

import org.apache.flink.streaming.api.windowing.time.Time;

public abstract class AbstractSlidingWindows extends AbstractTimeWindows {

  Time slide;

  protected AbstractSlidingWindows(Time size, Time slide, Time offset) {
    super(size, offset);
    this.slide = slide;
  }

}
