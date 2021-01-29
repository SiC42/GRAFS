package edu.leipzig.grafs.benchmark;

import edu.leipzig.grafs.benchmark.serialization.TripletDeserializer;
import edu.leipzig.grafs.model.Triplet;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class CitibikeConsumer {

  private final static String TOPIC = "citibike";
  private final static String BOOTSTRAP_SERVERS = "bdclu1.informatik.intern.uni-leipzig.de:9092";

  private static Properties createProperties(Properties properties) {
    var props = properties;
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "CitibikeConsumer" + Math.random());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TripletDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 30_000);
    return props;
  }

  private static Consumer<String, Triplet> createConsumer() {
    Consumer<String, Triplet> consumer = new KafkaConsumer<>(
        createProperties(new Properties()));
    consumer.subscribe(Collections.singletonList(TOPIC));
    return consumer;
  }

  private static void runConsumer() {
    var consumer = createConsumer();
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
