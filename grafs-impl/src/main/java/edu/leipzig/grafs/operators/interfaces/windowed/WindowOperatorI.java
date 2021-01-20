package edu.leipzig.grafs.operators.interfaces.windowed;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.AbstractWindowedStream.WindowInformation;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.windowing.windows.Window;

public interface WindowOperatorI {

  /**
   * Applies the given operator to the stream.
   *
   * @param stream stream on which the operator should be applied
   * @return the stream with the applied operator
   */
  <W extends Window> DataStream<Triplet> execute(DataStream<Triplet> stream,
      WindowInformation<W> wi);

  default <W extends Window> WindowedStream<Triplet, String, W> applyOtherWindowInformation(
      WindowedStream<Triplet, String, W> windowedStream, WindowInformation<W> wi) {
    if (wi.getTrigger() != null) {
      windowedStream = windowedStream.trigger(wi.getTrigger());
    }
    if (wi.getEvictor() != null) {
      windowedStream = windowedStream.evictor(wi.getEvictor());
    }
    if (wi.getLateness() != null) {
      windowedStream = windowedStream.allowedLateness(wi.getLateness());
    }
    if (wi.getOutputTag() != null) {
      windowedStream = windowedStream.sideOutputLateData(wi.getOutputTag());
    }
    return windowedStream;
  }

  default <W extends Window> AllWindowedStream<Triplet, W> applyOtherWindowInformation(
      AllWindowedStream<Triplet, W> windowedStream, WindowInformation<W> wi) {
    if (wi.getTrigger() != null) {
      windowedStream = windowedStream.trigger(wi.getTrigger());
    }
    if (wi.getEvictor() != null) {
      windowedStream = windowedStream.evictor(wi.getEvictor());
    }
    if (wi.getLateness() != null) {
      windowedStream = windowedStream.allowedLateness(wi.getLateness());
    }
    if (wi.getOutputTag() != null) {
      windowedStream = windowedStream.sideOutputLateData(wi.getOutputTag());
    }
    return windowedStream;
  }

}
