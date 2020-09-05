package edu.leipzig.grafs.benchmarking.generic;

import org.apache.flink.api.common.functions.FilterFunction;

public abstract class FilterFunctionWithMeter<T> extends FunctionWithMeter implements
    FilterFunction<T> {

  protected FilterFunctionWithMeter(String name) {
    super(name);
  }
}
