package edu.leipzig.grafs.connectors;

import com.google.common.util.concurrent.RateLimiter;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.common.TopicPartition;

public class RateLimitingKafkaConsumer<T> extends FlinkKafkaConsumer<T> {

  private final double topicRateLimit;
  private transient RateLimiter subtaskRateLimiter;

  public RateLimitingKafkaConsumer(String topic,
      DeserializationSchema<T> valueDeserializer,
      Properties props, double topicRateLimit) {
    super(topic, valueDeserializer, props);
    this.topicRateLimit = topicRateLimit;
  }

  @Override
  public void open(Configuration configuration) throws Exception {
    Preconditions.checkArgument(
        topicRateLimit / getRuntimeContext().getNumberOfParallelSubtasks() > 0.1,
        "subtask ratelimit should be greater than 0.1 QPS");
    subtaskRateLimiter = RateLimiter.create(
        topicRateLimit / getRuntimeContext().getNumberOfParallelSubtasks());
    super.open(configuration);
  }

  @Override
  protected AbstractFetcher<T, ?> createFetcher(
      SourceContext<T> sourceContext,
      Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
      SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
      StreamingRuntimeContext runtimeContext, OffsetCommitMode offsetCommitMode,
      MetricGroup consumerMetricGroup, boolean useMetrics) throws Exception {

    return new KafkaFetcher<>(
        sourceContext, assignedPartitionsWithInitialOffsets, watermarkStrategy,
        runtimeContext.getProcessingTimeService(),
        runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
        runtimeContext.getUserCodeClassLoader(), runtimeContext.getTaskNameWithSubtasks(),
        this.deserializer, this.properties, this.pollTimeout, runtimeContext.getMetricGroup(),
        consumerMetricGroup, useMetrics) {
      @Override
      protected void emitRecordsWithTimestamps(Queue<T> records,
          KafkaTopicPartitionState<T, TopicPartition> partitionState,
          long offset, long kafkaEventTimestamp) {
        synchronized (this.checkpointLock) {
          T record;
          while ((record = records.poll()) != null) {
            subtaskRateLimiter.acquire();
            long timestamp = partitionState.extractTimestamp(record, kafkaEventTimestamp);
            this.sourceContext.collectWithTimestamp(record, timestamp);
            partitionState.onEvent(record, timestamp);
          }
          partitionState.setOffset(offset);
        }
      }
    };
  }
}
