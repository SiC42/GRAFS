package edu.leipzig.grafs.benchmarking.generic;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;

public abstract class FlatMapFunctionWithMeter<IN, OUT> extends RichMapFunction<IN, OUT> {


  private final String name;

  protected FlatMapFunctionWithMeter(String name) {
    this.name = name;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
    getRuntimeContext()
        .getMetricGroup()
        .meter(name, new DropwizardMeterWrapper(dropwizardMeter));
  }
}