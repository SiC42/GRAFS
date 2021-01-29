package edu.leipzig.grafs.model.window;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public abstract class AbstractTimeWindows implements WindowsI<TimeWindow> {

  Time size;
  Time offset;

  protected AbstractTimeWindows(Time size, Time offset){
    if (offset != null && Math.abs(offset.toMilliseconds()) >= size.toMilliseconds()) {
      throw new IllegalArgumentException(
          "TumblingWindows parameters must satisfy abs(offset) < size");
    }
    this.size = size;
    this.offset = offset;
  }

}
