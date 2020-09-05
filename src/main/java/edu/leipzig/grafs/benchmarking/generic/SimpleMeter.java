package edu.leipzig.grafs.benchmarking.generic;

public class SimpleMeter<T> extends MapFunctionWithMeter<T, T> {

  public SimpleMeter(String name) {
    super(name);
  }

  @Override
  public T map(T in) throws Exception {
    return in;
  }
}
