package edu.leipzig.grafs.operators.interfaces.window;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.model.window.WindowingInformation;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

public interface WindowedOperatorI {

  /**
   * Applies the given operator to the stream.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the applied operator
   */
  <FW extends Window> DataStream<Triplet<Vertex, Edge>> execute(
      DataStream<Triplet<Vertex, Edge>> stream,
      WindowingInformation<FW> wi);

  default <FW extends Window, T extends Triplet<?, ?>> WindowedStream<T, String, FW> applyOtherWindowInformation(
      WindowedStream<T, String, FW> windowedStream, WindowingInformation<FW> wi) {
    if (wi.getTrigger() != null) {
      windowedStream = windowedStream.trigger(wi.getTrigger());
    }
    if (wi.getEvictor() != null) {
      windowedStream = windowedStream.evictor(wi.getEvictor());
    }
    if (wi.getLateness() != null) {
      windowedStream = windowedStream.allowedLateness(wi.getLateness());
    }
    return windowedStream;
  }

  default <FW extends Window, T extends Triplet<?, ?>> AllWindowedStream<T, FW> applyOtherWindowInformation(
      AllWindowedStream<T, FW> windowedStream, WindowingInformation<FW> wi) {
    if (wi.getTrigger() != null) {
      windowedStream = windowedStream.trigger(wi.getTrigger());
    }
    if (wi.getEvictor() != null) {
      windowedStream = windowedStream.evictor(wi.getEvictor());
    }
    if (wi.getLateness() != null) {
      windowedStream = windowedStream.allowedLateness(wi.getLateness());
    }
    return windowedStream;
  }

}
