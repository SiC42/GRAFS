package streaming.util;

import java.util.Objects;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkConfig {

  /**
   * Flink execution environment.
   */
  private final StreamExecutionEnvironment executionEnvironment;


  /**
   * Creates a new Configuration.
   *
   * @param executionEnvironment Flink execution environment
   */
  public FlinkConfig(StreamExecutionEnvironment executionEnvironment) {
    Objects.requireNonNull(executionEnvironment);
    this.executionEnvironment = executionEnvironment;
  }


  /**
   * Returns the Flink execution environment.
   *
   * @return Flink execution environment
   */
  public StreamExecutionEnvironment getExecutionEnvironment() {
    return executionEnvironment;
  }
}
