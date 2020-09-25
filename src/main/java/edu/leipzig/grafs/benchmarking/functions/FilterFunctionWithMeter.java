package edu.leipzig.grafs.benchmarking.functions;

import org.apache.flink.api.common.functions.FilterFunction;

public abstract class FilterFunctionWithMeter<T> extends FunctionWithMeter implements
    FilterFunction<T> {

  protected FilterFunctionWithMeter(String name) {
    super(name);
  }

  protected abstract boolean plainFilter(T t) throws Exception;

  @Override
  public boolean filter(T t) throws Exception {
    this.meter.markEvent();
    return plainFilter(t);
  }
}
