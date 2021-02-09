package edu.leipzig.grafs.model.window;

import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;

public abstract class AbstractTumblingWindows extends AbstractTimeWindows {

  WindowStagger windowStagger;

  protected AbstractTumblingWindows(Time size, Time offset, WindowStagger windowStagger) {
    super(size, offset);
    this.windowStagger = windowStagger;
  }

}
