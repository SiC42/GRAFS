package edu.leipzig.grafs.benchmark.tests;

import edu.leipzig.grafs.benchmark.serialization.TripletDeserializer;
import edu.leipzig.grafs.connectors.RateLimitingKafkaConsumer;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.model.streaming.StreamI;
import edu.leipzig.grafs.serialization.TripletDeserializationSchema;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public abstract class AbstractBenchmark {

  public static final String TOPIC_KEY = "topic";
  private static final String INPUT = "fileinput";
  private static final String KAFKA = "kafka";
  private static final String RATE_LIMIT = "ratelimit";
  private static final String INPUT_PARALLELISM = "inputp";

  protected StreamExecutionEnvironment env;
  protected StreamI stream;
  protected String operatorName;
  protected Writer outputWriter;
  protected int parallelism;
  private String outputPath;

  public AbstractBenchmark(String[] args) {
    init();
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    checkArgs(args);
  }

  private static Map<String, String> extractKafkaInformation(String kafkaAddress)
      throws ParseException {
    var kafkaAddressExpr = "^(.*):([\\d]{1,5})\\/([a-zA-Z0-9\\._\\-]+)$";
    var addressPattern = Pattern.compile(kafkaAddressExpr);
    var matcher = addressPattern.matcher(kafkaAddress);
    if (matcher.matches()) {
      String host = matcher.group(1);
      String port = matcher.group(2);
      String topic = matcher.group(3);

      final var validHostName = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])$";
      final var validIpAdrress = "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$";

      if (!host.matches(validHostName)
          && !host.matches(validIpAdrress)) {
        throw new ParseException(
            "Error parsing 'kafka'. Address is not a valid hostname or ip address. Please provide a valid server address via hostname:port/topic");
      }
      return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":" + port, TOPIC_KEY, topic);
    } else {
      throw new ParseException(
          String.format(
              "Error parsing 'kafka'. '%s' is not a valid kafka server address. Please provide a valid server address via hostname:port/topic",
              kafkaAddress));
    }
  }

  protected void init() {
    this.operatorName = getClass().getSimpleName();
    this.outputPath = "";
  }

  public void execute() throws Exception {
    var baseStream = applyOperator((GraphStream) stream);
    if (outputPath.equals("")) {
      baseStream.addSink(new DiscardingSink<>());
    } else {
      baseStream.getDataStream().writeAsText(outputPath);
    }
    var result = env.execute(this.operatorName);
    var timeInMilliSeconds = result.getNetRuntime(TimeUnit.MILLISECONDS);
    outputWriter.write(getCsvLine(timeInMilliSeconds));
    outputWriter.flush();
    outputWriter.close();
  }

  protected String getCsvLine(long timeInMilliSeconds) {
    return String.format("%s;-1;-1;%d\n", this.operatorName, timeInMilliSeconds);
  }

  private void checkArgs(String[] args) {
    var parser = new DefaultParser();
    var options = buildOptions();
    var header = String.format("Benchmarking GRAFS with %s.", operatorName);
    HelpFormatter formatter = new HelpFormatter();
    try {
      var cmd = parser.parse(options, args);
      if (cmd.hasOption("help")) {
        formatter.printHelp("grafsbenchmark", header, options, "");
      }

      // Process INPUT-stuff
      if (cmd.hasOption(INPUT) && cmd.hasOption(KAFKA)) {
        throw new ParseException(
            "Two inputs declared, but only one allowed. Either remove 'fileinput' or the kafka server information");
      }
      if (cmd.hasOption(INPUT)) {
        // do fileinput
        throw new ParseException("Error. File input not supported yet.");
      } else if (cmd.hasOption(KAFKA)) {
        if (cmd.hasOption(INPUT_PARALLELISM)) {
          try {
            parallelism = Integer.parseInt(cmd.getOptionValue(INPUT_PARALLELISM));
          } catch (NumberFormatException e) {
            throw new ParseException("Provided argument for input parallelism is not a number.");
          }
        } else {
          parallelism = -1;
        }
        int rateLimit;
        if (cmd.hasOption(RATE_LIMIT)) {
          try {
            rateLimit = Integer.parseInt(cmd.getOptionValue(RATE_LIMIT));
          } catch (NumberFormatException e) {
            throw new ParseException("Provided argument with 'ratelimit' is not a number.");
          }
        } else {
          rateLimit = -1;
        }
        // do kafka stuff
        var propsMap = new HashMap<>(
            extractKafkaInformation(cmd.getOptionValue(KAFKA)));
        buildStreamWithKafkaConsumer(propsMap, rateLimit);
      } else {
        throw new ParseException(
            "Missing input. Either declare a fileinput or provide the information to a kafka server");
      }

      // Process OUTPUT
      if (cmd.hasOption("output")) {
        outputPath =
            cmd.getOptionValue("output") + "output_" + LocalDateTime.now().toString() + ".log";
      }

      // Process LOG
      if (!cmd.hasOption("log")) {
        throw new ParseException("Missing parameter: log");
      } else {
        try {
          var fileOutputStream = new FileOutputStream(cmd.getOptionValue("log"), true);
          this.outputWriter = new OutputStreamWriter(fileOutputStream);
        } catch (IOException e) {
          e.printStackTrace();
          throw new ParseException("Unreadable output path");
        }
      }

    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("grafsbenchmark", header, options, "");

      System.exit(1);
    }
  }

  protected Options buildOptions() {
    var options = new Options();
    options.addOption("h", "help", false, "print this message");
    options.addOption("i", INPUT, true, "input file path");
    options.addOption("kip", KAFKA, true, "the kafka server in the format hostname:port/topic");
    options
        .addOption(INPUT_PARALLELISM, true, "maximum number of flink worker to read from source. Should not be larger than the number of kafka partitions");
    options
        .addOption("l", RATE_LIMIT, true, "the rate limit for the intake of data into the system");
    options.addOption("o", "output", true, "location for the output file");
    options.addOption("log", true, "location for the log file");
    return options;
  }

  private void buildStreamWithKafkaConsumer(Map<String, String> map, int rateLimit) {
    var properties = createProperties(map.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    var schema = new TripletDeserializationSchema();
    var config = new FlinkConfigBuilder(env).build();
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    if (rateLimit > 0) {
      var kafkaConsumer = new RateLimitingKafkaConsumer<>(map.get(TOPIC_KEY), schema, properties,
          rateLimit);
      stream = GraphStream.fromSource(kafkaConsumer, config, parallelism);
    } else {
      stream = GraphStream
          .fromSource(new FlinkKafkaConsumer<>(map.get(TOPIC_KEY), schema, properties),
              config, parallelism);
    }
  }

  private Properties createProperties(String bootstrapServerConfig) {
    var props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "CitibikeConsumer" + Math.random());
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        TripletDeserializer.class.getName());
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 30_000);
    return props;
  }

  public abstract AbstractStream applyOperator(GraphStream stream);
}
