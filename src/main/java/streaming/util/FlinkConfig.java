package streaming.util;

import java.util.Objects;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import streaming.model.EdgeContainer;

public class FlinkConfig {

  /**
   * Flink execution environment.
   */
  private final StreamExecutionEnvironment executionEnvironment;

  /**
   * Strategy used to determine the timestamp of elements in the Flink stream
   */
  private final WatermarkStrategy<EdgeContainer> watermarkStrategy;

  /**
   * Creates a new Configuration.
   *
   * @param executionEnvironment Flink execution environment
   */
  protected FlinkConfig(StreamExecutionEnvironment executionEnvironment,
      WatermarkStrategy<EdgeContainer> watermarkStrategy) {
    this.executionEnvironment = Objects.requireNonNull(executionEnvironment,
        "execution environment must not be null");
    this.watermarkStrategy = watermarkStrategy;
  }

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
  public WatermarkStrategy<EdgeContainer> getWatermarkStrategy() {
    return watermarkStrategy;
  }
}
