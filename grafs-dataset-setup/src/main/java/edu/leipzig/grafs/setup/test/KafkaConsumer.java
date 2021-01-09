package edu.leipzig.grafs.setup.test;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.setup.serialization.TripletDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


/**
 * Simple tests class, reads the given kafka consumer and prints the last 10 ECs.
 */
public class KafkaConsumer {

  private final static String TOPIC = "citibike";
  private final static String BOOTSTRAP_SERVERS = "kafka:9092";

  public static Properties createProperties(Properties properties) {
    var props = properties;
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CitibikeConsumer" + Math.random());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        TripletDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 30_000);
    return props;
  }

  public static Consumer<String, Triplet> createConsumer() {
    Consumer<String, Triplet> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(
        createProperties(new Properties()));
    consumer.subscribe(Collections.singletonList(TOPIC));
    return consumer;
  }

  static void runConsumer() {
    var consumer = createConsumer();
    int noMessageFound = 0;
    var tripletQueue = new SmallQueue();
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
        tripletQueue.add(record.value());
      }
      // commits the offset of record to broker.
      consumer.commitAsync();
    }
    consumer.close();
    System.out.println("Parsed: " + parsed);
    tripletQueue.asColl().forEach(System.out::println);
  }

  public static void main(String[] args) {
    runConsumer();
  }

}
