package edu.leipzig.grafs.benchmark.operators.functions;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;

abstract class FunctionWithMeter extends AbstractRichFunction {

  private final String name;

  protected transient Meter meter;

  protected FunctionWithMeter(String name) {
    this.name = name;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
    this.meter = getRuntimeContext()
        .getMetricGroup()
        .meter(name, new DropwizardMeterWrapper(dropwizardMeter));
  }

}
