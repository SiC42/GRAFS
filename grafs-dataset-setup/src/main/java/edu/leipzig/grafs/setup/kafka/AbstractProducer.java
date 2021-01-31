package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.serialization.TripletDeserializationSchema;
import edu.leipzig.grafs.setup.AbstractCmdBase;
import edu.leipzig.grafs.setup.serialization.TripletSerializer;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;

public abstract class AbstractProducer extends AbstractCmdBase {


  protected List<PartitionInfo> partitions;
  private final String KAFKA_TOPIC_KEY;
  protected final String serverInfo;

  public AbstractProducer(String[] args) {
    super(args);
    serverInfo = properties.getProperty("bootstrap.servers");
    KAFKA_TOPIC_KEY = properties.getProperty(TOPIC_KEY);
    getPartitionInformation();

  }

  protected void getPartitionInformation() {
    var producer = buildProducer("onlypartitioninfo", serverInfo);
    partitions = producer.partitionsFor(KAFKA_TOPIC_KEY);
    System.out.printf("Found %d partitions on topic '%s'.\n", partitions.size(), KAFKA_TOPIC_KEY);
  }

  protected static Producer<String, Triplet> buildProducer(String client, String serverInfo) {
    Properties kafkaProps = new Properties();
    kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverInfo);
    kafkaProps.put(ProducerConfig.CLIENT_ID_CONFIG, client + "-CsvToKafkaProducer");
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        TripletSerializer.class.getName());
    return new org.apache.kafka.clients.producer.KafkaProducer<>(kafkaProps);
  }


  protected void sendTriplet(Producer<String, Triplet> producer, Triplet triplet)
      throws ExecutionException, InterruptedException {
    final var record = new ProducerRecord<>(
        properties.getProperty(TOPIC_KEY), triplet.getEdge().getId().toString(), triplet);
    RecordMetadata metadata = producer.send(record).get();
  }

  protected void sendEndOfStreamToAllPartitions() throws ExecutionException, InterruptedException {
    var producer = buildProducer("finaltripletproducer", serverInfo);
    for(var info : partitions){
      int partitionNumber = info.partition();
      var eosTriplet = createEndOfStreamTriplet();
      final var record = new ProducerRecord<>(KAFKA_TOPIC_KEY, partitionNumber, eosTriplet.getEdge().getId().toString(), eosTriplet);
      producer.send(record);
    }
  }

  private Triplet createEndOfStreamTriplet(){
    // send a last object that is not part of the analysis, but marks end of stream
    var source = new Vertex();
    var END_OF_STREAM_LABEL = TripletDeserializationSchema.END_OF_STREAM_LABEL;
    source.setLabel(END_OF_STREAM_LABEL);
    var target = new Vertex();
    target.setLabel(END_OF_STREAM_LABEL);
    var edge = EdgeFactory.createEdge(source, target);
    edge.setLabel(END_OF_STREAM_LABEL);
    return new Triplet(edge, source, target);
  }

}
