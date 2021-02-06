package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.serialization.SimpleStringSchemaWithEnd;
import edu.leipzig.grafs.setup.AbstractCmdBase;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;

public abstract class AbstractProducer extends AbstractCmdBase {


  protected Producer<String, String> producer;
  protected List<PartitionInfo> partitions;
  private final String KAFKA_TOPIC_KEY;
  public String END_OF_STREAM_LABEL = SimpleStringSchemaWithEnd.END_OF_STREAM_LABEL;

  public AbstractProducer(String[] args) {
    super(args);
    KAFKA_TOPIC_KEY = properties.getProperty(TOPIC_KEY);
    buildProducer();
    getPartitionInformation();

  }

  protected void getPartitionInformation() {
    partitions = producer.partitionsFor(KAFKA_TOPIC_KEY);
    System.out.printf("Found %d partitions on topic '%s'.\n", partitions.size(), KAFKA_TOPIC_KEY);
  }

  private void buildProducer() {
    System.out.println("Initializing Producer");
    Properties kafkaProps = new Properties();
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrap.servers"));
    kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, "CsvToKafkaProducer");
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(kafkaProps);
  }


  protected void sendTriplet(String id, String triplet)
      throws ExecutionException, InterruptedException {
    final var record = new ProducerRecord<>(
        properties.getProperty(TOPIC_KEY), id, triplet);
    RecordMetadata metadata = producer.send(record).get();
  }

  protected void sendEndOfStreamToAllPartitions() throws ExecutionException, InterruptedException {
    for(var info : partitions){
      int partitionNumber = info.partition();
      final var record = new ProducerRecord<>(KAFKA_TOPIC_KEY, partitionNumber, END_OF_STREAM_LABEL, END_OF_STREAM_LABEL );
      producer.send(record).get();
    }
  }

}
