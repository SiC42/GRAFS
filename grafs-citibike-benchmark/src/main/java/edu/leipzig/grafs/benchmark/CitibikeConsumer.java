package edu.leipzig.grafs.benchmark;

import edu.leipzig.grafs.model.Triplet;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class CitibikeConsumer {

  private final static String TOPIC = "citibike";
  private final static String BOOTSTRAP_SERVERS = "bdclu1.informatik.intern.uni-leipzig.de:9092";

  public static Properties createProperties(String bootstrapServerConfig) {
    var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CitibikeConsumer" + Math.random());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 30_000);
    return props;
  }

  public static Consumer<String, String> createConsumer(Properties properties) {
    Consumer<String, String> consumer = new KafkaConsumer<>(
        createProperties(properties.getProperty("bootstrap.servers")));
    consumer.subscribe(List.of(properties.getProperty("topic")));
    return consumer;
  }

  private static void runConsumer() {
    var properties = new Properties();
    properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
    properties.put("topic", TOPIC);
    var consumer = createConsumer(properties);
    int noMessageFound = 0;
    while (true) {
      var consumerRecords = consumer.poll(Duration.ofMillis(1000));
      // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
      if (consumerRecords.count() == 0) {
        noMessageFound++;
        if (noMessageFound > 100) {
          System.out.println("No message received");
          // If no message found count is reached to threshold exit loop.
          break;
        } else {
          continue;
        }
      }
      //print each record.
      System.out.println((consumerRecords.count()));
      consumerRecords.forEach(record -> {
        System.out.println("Record Key " + record.key());
        System.out.println("Record value " + record.value());
        System.out.println("Record partition " + record.partition());
        System.out.println("Record offset " + record.offset());
      });
      // commits the offset of record to broker.
      consumer.commitAsync();
    }
    consumer.close();
  }

  public static void main(String[] args) {
    runConsumer();
  }

}
