package edu.leipzig.grafs.benchmarking.generic;

import org.apache.flink.api.common.functions.MapFunction;

public abstract class MapFunctionWithMeter<IN, OUT> extends FunctionWithMeter implements
    MapFunction<IN, OUT> {

  public MapFunctionWithMeter(String name) {
    super(name);
  }
}