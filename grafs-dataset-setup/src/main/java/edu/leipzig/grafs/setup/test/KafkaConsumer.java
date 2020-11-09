package edu.leipzig.grafs.setup.test;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.setup.serialization.EdgeContainerDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumer {

  private final static String TOPIC = "citibike";
  private final static String BOOTSTRAP_SERVERS = "kafka:9092";

  public static Properties createProperties(Properties properties) {
    var props = properties;
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CitibikeConsumer" + Math.random());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        EdgeContainerDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 30_000);
    return props;
  }

  public static Consumer<String, EdgeContainer> createConsumer() {
    Consumer<String, EdgeContainer> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
        createProperties(new Properties()));
    consumer.subscribe(Collections.singletonList(TOPIC));
    return consumer;
  }

  static void runConsumer() {
    var consumer = createConsumer();
    int noMessageFound = 0;
    var ecQueue = new SmallQueue();
    int parsed = 0;
    while (true) {
      var consumerRecords = consumer.poll(Duration.ofMillis(1000));
      // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
      if (consumerRecords.count() == 0) {
        noMessageFound++;
        if (noMessageFound > 10) {
          System.out.println("No message received");
          // If no message found count is reached to threshold exit loop.
          break;
        } else {
          continue;
        }
      }

      for (var record : consumerRecords) {
        parsed++;
        if (parsed % 10000 == 0) {
          System.out.print(parsed + "\r");
          System.out.flush();
        }
        ecQueue.add(record.value());
      }
      // commits the offset of record to broker.
      consumer.commitAsync();
    }
    consumer.close();
    System.out.println("Parsed: " + parsed);
    ecQueue.asColl().forEach(System.out::println);
  }

  public static void main(String[] args) {
    runConsumer();
  }

}
