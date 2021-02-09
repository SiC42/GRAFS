package edu.leipzig.grafs.util;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import java.util.Objects;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkConfig {

  /**
   * Flink execution environment.
   */
  private final StreamExecutionEnvironment executionEnvironment;

  /**
   * Strategy used to determine the timestamp of elements in the Flink stream
   */
  private final WatermarkStrategy<Triplet<Vertex, Edge>> watermarkStrategy;

  /**
   * Creates a new Configuration.
   *
   * @param executionEnvironment Flink execution environment
   */
  protected FlinkConfig(StreamExecutionEnvironment executionEnvironment,
      WatermarkStrategy<Triplet<Vertex, Edge>> watermarkStrategy) {
    this.executionEnvironment = Objects.requireNonNull(executionEnvironment,
        "execution environment must not be null");
    this.watermarkStrategy = watermarkStrategy;
  }

  /**
   * Allows building a config via the Flink config builder
   *
   * @param executionEnvironment Flink execution environment
   * @return flink config builder object used to build this config
   */
  public static FlinkConfigBuilder buildNewConfig(StreamExecutionEnvironment executionEnvironment) {
    Objects.requireNonNull(executionEnvironment, "execution environment must not be null");
    return new FlinkConfigBuilder(executionEnvironment);
  }

  /**
   * Returns the Flink execution environment.
   *
   * @return Flink execution environment
   */
  public StreamExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }


  /**
   * Returns strategy used to determine the timestamp of elements in the Flink stream
   */
  public WatermarkStrategy<Triplet<Vertex, Edge>> getWatermarkStrategy() {
    return watermarkStrategy;
  }
}
