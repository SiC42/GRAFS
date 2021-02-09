package edu.leipzig.grafs.util;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Allows the creation of flink configs.
 */
public class FlinkConfigBuilder {

  /**
   * Flink execution environment.
   */
  private final StreamExecutionEnvironment executionEnvironment;

  /**
   * Strategy used to determine the timestamp of elements in the Flink stream
   */
  private WatermarkStrategy<Triplet<Vertex, Edge>> watermarkStrategy;

  /**
   * Constructs a flink config builder using the given execution environment.
   *
   * @param executionEnvironment Flink execution environment
   */
  public FlinkConfigBuilder(StreamExecutionEnvironment executionEnvironment) {
    this.executionEnvironment = executionEnvironment;
    watermarkStrategy = WatermarkStrategy.noWatermarks();
  }

  /**
   * Used to specify the watermark strategy used in Flink streams.
   *
   * @param watermarkStrategy specifies the watermark strategy used by flink
   * @return this object with the water mark strategy set
   */
  public FlinkConfigBuilder withWaterMarkStrategy(
      WatermarkStrategy<Triplet<Vertex, Edge>> watermarkStrategy) {
    this.watermarkStrategy = watermarkStrategy;
    return this;
  }

  /**
   * Used to specify the time characteristic used in Flink streams.
   *
   * @param timeCharacteristic specifies the time characteristic used by flink
   * @return this object with the time characteristic set
   */
  public FlinkConfigBuilder withTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
    executionEnvironment.setStreamTimeCharacteristic(timeCharacteristic);
    return this;
  }

  /**
   * Builds the flink config with the provided information.
   *
   * @return the flink config with the provided information
   */
  public FlinkConfig build() {
    return new FlinkConfig(executionEnvironment, watermarkStrategy);
  }
}
