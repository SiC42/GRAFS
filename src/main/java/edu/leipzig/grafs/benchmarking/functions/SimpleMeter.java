package edu.leipzig.grafs.benchmarking.functions;

public class SimpleMeter<T> extends MapFunctionWithMeter<T, T> {

  public SimpleMeter(String name) {
    super(name);
  }

  @Override
  public T plainMap(T in) throws Exception {
    return in;
  }
}
