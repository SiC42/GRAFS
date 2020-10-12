package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.setup.reader.SerializedEdgeContainerFileReader;
import edu.leipzig.grafs.setup.serialization.EdgeContainerSerializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class FileToKafkaProducer {

  private final static String TOPIC = "citibike";
  private final static String BOOTSTRAP_SERVERS = "kafka:9092";

  private static Producer<String, EdgeContainer> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        EdgeContainerSerializer.class.getName());
    return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
  }

  static void runProducer() throws Exception {
    var producer = createProducer();
    try (var reader = new SerializedEdgeContainerFileReader()) {
      var lineCount = reader.getNumberOfLines();
      double curLine = 0;
      while (reader.hasNext()) {
        curLine++;
        if (curLine % 5000 == 0) {
          var relativeProgress = Math.round(curLine * 100 / lineCount);
          System.out.print(" Progress: " + relativeProgress + "%\r");
          System.out.flush();
        }
        var ec = reader.getNext();
        final var record = new ProducerRecord<>(TOPIC, ec.getEdge().getId().toString(), ec);
        RecordMetadata metadata = producer.send(record).get();
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.flush();
      producer.close();
    }
  }

  public static void main(String... args) throws Exception {
    if (args.length == 0) {
      runProducer();
    }
  }

}
