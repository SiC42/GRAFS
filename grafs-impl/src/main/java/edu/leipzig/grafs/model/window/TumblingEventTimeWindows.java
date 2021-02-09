package edu.leipzig.grafs.model.window;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TumblingEventTimeWindows extends AbstractTumblingWindows {


  protected TumblingEventTimeWindows(Time size, Time offset, WindowStagger windowStagger) {
    super(size, offset, windowStagger);
  }


  /**
   * Creates a new {@code TumblingEventTimeWindows} {@link WindowAssigner} that assigns elements to
   * time windows based on the element timestamp.
   *
   * @param size The size of the generated windows.
   * @return The time policy.
   */
  public static TumblingEventTimeWindows of(Time size) {
    return new TumblingEventTimeWindows(size, null, null);
  }

  /**
   * Creates a new {@code TumblingEventTimeWindows} {@link WindowAssigner} that assigns elements to
   * time windows based on the element timestamp and offset.
   *
   * <p>For example, if you want window a stream by hour,but window begins at the 15th minutes of
   * each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get time
   * windows start at 0:15:00,1:15:00,2:15:00,etc.
   *
   * <p>Rather than that,if you are living in somewhere which is not using UTC±00:00 time, such as
   * China which is using UTC+08:00,and you want a time window with size of one day, and window
   * begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}. The
   * parameter of offset is {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than UTC
   * time.
   *
   * @param size   The size of the generated windows.
   * @param offset The offset which window start would be shifted by.
   * @return The time policy.
   */
  public static TumblingEventTimeWindows of(Time size, Time offset) {
    return new TumblingEventTimeWindows(size, offset, null);
  }

  /**
   * Creates a new {@code TumblingEventTimeWindows} {@link WindowAssigner} that assigns elements to
   * time windows based on the element timestamp, offset and a staggering offset, depending on the
   * staggering policy.
   *
   * @param size          The size of the generated windows.
   * @param offset        The offset which window start would be shifted by.
   * @param windowStagger The utility that produces staggering offset in runtime.
   * @return The time policy.
   */
  @PublicEvolving
  public static TumblingEventTimeWindows of(
      Time size, Time offset, WindowStagger windowStagger) {
    return new TumblingEventTimeWindows(size, offset, windowStagger);
  }

  @Override
  public WindowAssigner<Object, TimeWindow> getFlinkWindowAssigner() {
    if (offset == null) {
      return org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.of(size);
    } else if (windowStagger == null) {
      return org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
          .of(size, offset);
    } else {
      return org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
          .of(size, offset, windowStagger);
    }
  }
}
