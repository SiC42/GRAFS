package edu.leipzig.grafs.benchmark.util;

import edu.leipzig.grafs.benchmark.CitibikeConsumer;
import edu.leipzig.grafs.benchmark.serialization.EdgeContainerDeserializer;
import edu.leipzig.grafs.connectors.RateLimitingKafkaConsumer;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.serialization.EdgeContainerDeserializationSchema;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerBuilder {



}
