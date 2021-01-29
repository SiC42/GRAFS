package edu.leipzig.grafs.model.window;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class SlidingEventTimeWindows extends AbstractSlidingWindows {



  protected SlidingEventTimeWindows(Time size, Time slide, Time offset){
    super(size, slide, offset);
  }

  public static SlidingEventTimeWindows of(Time size, Time slide) {
    return new SlidingEventTimeWindows(size, slide, null);
  }

  @PublicEvolving
  public static SlidingEventTimeWindows of(
      Time size, Time slide, Time offset) {
    return new SlidingEventTimeWindows(size, slide, offset);
  }

  @Override
  public WindowAssigner<Object, TimeWindow> getFlinkWindowAssigner() {
    if(offset == null){
      return org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows.of(size, slide);
    } else{
      return org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows.of(size, slide, offset);
    }
  }
}
