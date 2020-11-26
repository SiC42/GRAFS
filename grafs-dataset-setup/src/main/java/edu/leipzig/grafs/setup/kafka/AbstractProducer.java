package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.setup.AbstractCmdBase;
import edu.leipzig.grafs.setup.serialization.EdgeContainerSerializer;
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
import org.apache.kafka.common.serialization.StringSerializer;

public abstract class AbstractProducer extends AbstractCmdBase {

  private static final String KAFKA = "kafka";

  protected String TOPIC;
  protected String BOOTSTRAP_SERVERS;
  protected Producer<String, EdgeContainer> producer;

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
      } else {
        throw new ParseException(
            "Missing input.Provide the information to a kafka server");
      }
    } catch (ParseException e) {
      formatter.printHelp("grafs-data-setup", header, options, "");

      System.exit(1);
    }
  }

  private void buildProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "CsvToKafkaProducer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        EdgeContainerSerializer.class.getName());
    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
  }


  protected void sendEdgeContainer(EdgeContainer ec)
      throws ExecutionException, InterruptedException {
    final var record = new ProducerRecord<>(TOPIC, ec.getEdge().getId().toString(), ec);
    RecordMetadata metadata = producer.send(record).get();
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
