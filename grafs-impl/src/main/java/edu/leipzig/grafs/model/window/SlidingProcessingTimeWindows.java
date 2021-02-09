package edu.leipzig.grafs.model.window;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class SlidingProcessingTimeWindows extends AbstractSlidingWindows {


  protected SlidingProcessingTimeWindows(Time size, Time slide, Time offset) {
    super(size, slide, offset);
  }

  public static SlidingProcessingTimeWindows of(Time size, Time slide) {
    return new SlidingProcessingTimeWindows(size, slide, null);
  }

  @PublicEvolving
  public static SlidingProcessingTimeWindows of(
      Time size, Time slide, Time offset) {
    return new SlidingProcessingTimeWindows(size, slide, offset);
  }

  @Override
  public WindowAssigner<Object, TimeWindow> getFlinkWindowAssigner() {
    if (offset == null) {
      return org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
          .of(size, slide);
    } else {
      return org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
          .of(size, slide, offset);
    }
  }
}
