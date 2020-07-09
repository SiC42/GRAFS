package streaming.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.EdgeContainer;

public class FlinkConfigBuilder {

  /**
   * Flink execution environment.
   */
  private final StreamExecutionEnvironment executionEnvironment;

  /**
   * Strategy used to determine the timestamp of elements in the Flink stream
   */
  private WatermarkStrategy<EdgeContainer> watermarkStrategy;

  public FlinkConfigBuilder(StreamExecutionEnvironment executionEnvironment) {
    this.executionEnvironment = executionEnvironment;
    watermarkStrategy = WatermarkStrategy.noWatermarks();
  }

  public FlinkConfigBuilder withWaterMarkStrategy(WatermarkStrategy<EdgeContainer> watermarkStrategy) {
    this.watermarkStrategy = watermarkStrategy;
    return this;
  }

  public FlinkConfigBuilder withTimeCharacteristic(TimeCharacteristic timeCharacteristic){
    executionEnvironment.setStreamTimeCharacteristic(timeCharacteristic);
    return this;
  }

  public FlinkConfig build(){
    return new FlinkConfig(executionEnvironment, watermarkStrategy);
  }
}
