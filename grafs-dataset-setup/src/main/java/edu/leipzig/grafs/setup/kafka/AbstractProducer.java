package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.setup.serialization.EdgeContainerSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public abstract class AbstractProducer {

  protected final String TOPIC = "citibike";
  protected final String BOOTSTRAP_SERVERS = "kafka:9092";
  protected Producer<String, EdgeContainer> producer;

  public AbstractProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
        BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "CsvToKafkaProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        EdgeContainerSerializer.class.getName());
    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
  }

  public abstract void run();

  protected void sendEdgeContainer(EdgeContainer ec)
      throws ExecutionException, InterruptedException {
    final var record = new ProducerRecord<>(TOPIC, ec.getEdge().getId().toString(), ec);
    RecordMetadata metadata = producer.send(record).get();
  }

}
