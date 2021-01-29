package edu.leipzig.grafs.model.window;


import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

public interface WindowsI<W extends Window> {


  WindowAssigner<Object, W> getFlinkWindowAssigner();

}
