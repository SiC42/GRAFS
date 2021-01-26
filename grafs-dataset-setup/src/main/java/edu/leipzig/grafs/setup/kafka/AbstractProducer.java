package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.serialization.TripletDeserializationSchema;
import edu.leipzig.grafs.setup.AbstractCmdBase;
import edu.leipzig.grafs.setup.serialization.TripletSerializer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
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

  private static final String KAFKA = "kafka";

  protected String TOPIC;
  protected String BOOTSTRAP_SERVERS;
  protected Producer<String, Triplet> producer;
  protected List<PartitionInfo> partitions;

  public AbstractProducer(String[] args) {
    super(args);
    checkArgs(args);
  }

  private void checkArgs(String[] args) {
    var parser = new DefaultParser();
    var options = buildOptions();
    var header = "Setup data for Benchmarking GRAFS";
    HelpFormatter formatter = new HelpFormatter();
    try {
      var cmd = parser.parse(options, args);
      if (cmd.hasOption(KAFKA)) {
        extractKafkaInformation(cmd.getOptionValue(KAFKA));
        buildProducer();
        getPartitionInformation();
      } else {
        throw new ParseException(
            "Missing input.Provide the information to a kafka server");
      }
    } catch (ParseException e) {
      formatter.printHelp("grafs-data-setup", header, options, "");

      System.exit(1);
    }
  }

  protected void getPartitionInformation() {
    partitions = producer.partitionsFor(TOPIC);
    System.out.printf("Found %d partitions on topic '%s'.\n", partitions.size(), TOPIC);
  }

  private void buildProducer() {
    System.out.println("Initializing Producer");
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "CsvToKafkaProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        TripletSerializer.class.getName());
    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
  }


  protected void sendTriplet(Triplet triplet)
      throws ExecutionException, InterruptedException {
    final var record = new ProducerRecord<>(TOPIC, triplet.getEdge().getId().toString(), triplet);
    RecordMetadata metadata = producer.send(record).get();
  }

  protected void sendEndOfStreamToAllPartitions() throws ExecutionException, InterruptedException {
    for(var info : partitions){
      int partitionNumber = info.partition();
      var eosTriplet = createEndOfStreamTriplet();
      final var record = new ProducerRecord<>(TOPIC, partitionNumber, eosTriplet.getEdge().getId().toString(), eosTriplet);
      RecordMetadata metadata = producer.send(record).get();
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

  protected Options buildOptions() {
    var options = super.buildOptions();
    options.addOption("kip", KAFKA, true, "the kafka server in the format hostname:port/topic");
    return options;
  }

  private void extractKafkaInformation(String kafkaAddress)
      throws ParseException {
    var kafkaAddressExpr = "^(.*):([\\d]{1,5})\\/([a-zA-Z0-9\\._\\-]+)$";
    var addressPattern = Pattern.compile(kafkaAddressExpr);
    var matcher = addressPattern.matcher(kafkaAddress);
    if (matcher.matches()) {
      String host = matcher.group(1);
      String port = matcher.group(2);
      String topic = matcher.group(3);

      final var validHostName = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])$";
      final var validIpAddress = "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$";

      if (!host.matches(validHostName)
          && !host.matches(validIpAddress)) {
        throw new ParseException(
            "Error parsing 'kafka'. Address is not a valid hostname or ip address. Please provide a valid server address via hostname:port/topic");
      }
      BOOTSTRAP_SERVERS = host + ":" + port;
      TOPIC = topic;
    } else {
      throw new ParseException(
          String.format(
              "Error parsing 'kafka'. '%s' is not a valid kafka server address. Please provide a valid server address via hostname:port/topic",
              kafkaAddress));
    }
  }

}
