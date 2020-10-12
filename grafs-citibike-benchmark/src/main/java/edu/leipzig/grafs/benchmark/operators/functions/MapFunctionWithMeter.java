package edu.leipzig.grafs.benchmark.operators.functions;

import edu.leipzig.grafs.benchmarking.functions.FunctionWithMeter;
import org.apache.flink.api.common.functions.MapFunction;

public abstract class MapFunctionWithMeter<IN, OUT> extends
    edu.leipzig.grafs.benchmarking.functions.FunctionWithMeter implements
    MapFunction<IN, OUT> {


  public MapFunctionWithMeter(String name) {
    super(name);
  }

  protected abstract OUT plainMap(IN in) throws Exception;

  @Override
  public OUT map(IN in) throws Exception {
    this.meter.markEvent();
    return plainMap(in);
  }
}