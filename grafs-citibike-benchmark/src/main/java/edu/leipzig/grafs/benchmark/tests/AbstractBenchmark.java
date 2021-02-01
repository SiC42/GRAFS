package edu.leipzig.grafs.benchmark.tests;

import edu.leipzig.grafs.benchmark.config.ProducerConfig;
import edu.leipzig.grafs.benchmark.serialization.TripletDeserializer;
import edu.leipzig.grafs.connectors.RateLimitingKafkaConsumer;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.serialization.TripletDeserializationSchema;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
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
  public static final String OPERATOR_NAME_KEY = "operatorname";
  public static final String OUTPUT_PATH_KEY = "output";

  private static final String CMD_INPUT = "fileinput";
  private static final String CMD_KAFKA = "kafka";
  private static final String CMD_RATE_LIMIT = "ratelimit";
  private static final String CMD_INPUT_PARALLELISM = "inputp";
  private static final String CMD_CONFIG = "config";

  protected StreamExecutionEnvironment env;
  protected AbstractStream<?> stream;
  protected Writer outputWriter;
  protected Properties properties;

  public AbstractBenchmark(String[] args) {
    properties = new Properties();
    properties.put(OPERATOR_NAME_KEY, getClass().getSimpleName());
    this.env = StreamExecutionEnvironment.getExecutionEnvironment();
    checkArgs(args);
    System.out.println("Loaded Properties:");
    System.out.println(properties);
  }

  private void checkArgs(String[] args) {
    var parser = new DefaultParser();
    var options = buildOptions();
    var header = String
        .format("Benchmarking GRAFS with %s.", properties.getProperty(OPERATOR_NAME_KEY));
    HelpFormatter formatter = new HelpFormatter();
    try {
      var cmd = parser.parse(options, args);
      if (cmd.hasOption("help")) {
        formatter.printHelp("grafsbenchmark", header, options, "");
      }
      if (cmd.hasOption(CMD_CONFIG)) {
        try {
          properties.putAll(ProducerConfig.loadProperties(cmd.getOptionValue(CMD_CONFIG)));
        } catch (IOException ioE) {
          System.err.println("Could not find provided config.");
        }
      } else {
        properties.putAll(ProducerConfig.loadDefaultProperties());
      }
      int numOfPartitions = getNumberOfPartitions(createKafkaProperties(properties.getProperty("bootstrap.servers")));
      System.out.format("Found %d partitions.\n", numOfPartitions);
      if (cmd.hasOption(CMD_INPUT_PARALLELISM)) {
        try {
          var parallelism = Integer.parseInt(cmd.getOptionValue(CMD_INPUT_PARALLELISM));
          if(parallelism > numOfPartitions){
            throw new ParseException("Provided number is greater than number of partitions");
          }
          properties.put(CMD_INPUT_PARALLELISM, String.valueOf(parallelism));
        } catch (NumberFormatException e) {
          throw new ParseException("Provided argument for input parallelism is not a number.");
        }
      } else {
        properties.put(CMD_INPUT_PARALLELISM, String.valueOf(numOfPartitions));
      }
      int rateLimit;
      if (cmd.hasOption(CMD_RATE_LIMIT)) {
        try {
          rateLimit = Integer.parseInt(cmd.getOptionValue(CMD_RATE_LIMIT));
          properties.put(CMD_RATE_LIMIT, cmd.getOptionValue(CMD_RATE_LIMIT));
        } catch (NumberFormatException e) {
          throw new ParseException("Provided argument with 'ratelimit' is not a number.");
        }
      } else {
        rateLimit = -1;
        properties.put(CMD_RATE_LIMIT, "None");
      }
      // do kafka stuff
      buildStreamWithKafkaConsumer(rateLimit);

      // Process OUTPUT
      if (cmd.hasOption("output")) {
        properties.put(OUTPUT_PATH_KEY,
            cmd.getOptionValue("output") + "output_" + System.currentTimeMillis());
      }

      // Process LOG
      String logPath;
      if (cmd.hasOption("log")) {
        properties.put("log", cmd.getOptionValue("log"));
      }

      logPath = properties.getProperty("log");

      try {
        var fileOutputStream = new FileOutputStream(logPath, true);
        this.outputWriter = new OutputStreamWriter(fileOutputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new ParseException("Unreadable output path");
      }

    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("grafs-data-setup", header, options, "");
      System.exit(1);
    }

  }

  public void execute() throws Exception {
    var baseStream = applyOperator((GraphStream) stream);
    if (properties.contains(OUTPUT_PATH_KEY)) {
      baseStream.getDataStream().writeAsText(properties.getProperty(OUTPUT_PATH_KEY));
    } else {
      baseStream.addSink(new DiscardingSink<>());
    }
    var result = env.execute(properties.getProperty(OPERATOR_NAME_KEY));
    var timeInMilliSeconds = result.getNetRuntime(TimeUnit.MILLISECONDS);
    outputWriter.write(getCsvLine(timeInMilliSeconds));
    outputWriter.flush();
    outputWriter.close();
  }

  private String getCsvLine(long timeInMilliSeconds) {
    return getCsvLine(timeInMilliSeconds, -1);
  }

  protected String getCsvLine(long timeInMilliSeconds, int windowSize) {
    var outputTypeStr = properties.contains(OUTPUT_PATH_KEY) ? "w" : "d";
    return String
        .format("%s;%s;%d;%d\n", properties.getProperty(OPERATOR_NAME_KEY), outputTypeStr, windowSize, timeInMilliSeconds);
  }

  protected Options buildOptions() {
    var options = new Options();
    options.addOption("h", "help", false, "print this message");
    options.addOption("i", CMD_INPUT, true, "input file path");
    options.addOption("kip", CMD_KAFKA, true, "the kafka server in the format hostname:port/topic");
    options
        .addOption(CMD_INPUT_PARALLELISM, true,
            "maximum number of flink worker to read from source. Has to me smaller than the number of topic partitions on the kafka server");
    options
        .addOption("l", CMD_RATE_LIMIT, true,
            "the rate limit for the intake of data into the system");
    options.addOption("o", "output", true, "location for the output file");
    options.addOption("log", true, "location for the log file");
    return options;
  }

  private void buildStreamWithKafkaConsumer(int rateLimit) {
    var localProps = createKafkaProperties(properties.getProperty("bootstrap.servers"));
    var schema = new TripletDeserializationSchema();
    var config = new FlinkConfigBuilder(env).build();
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    var topic = properties.getProperty(TOPIC_KEY);
    int parallelism = Integer.parseInt(properties.getProperty(CMD_INPUT_PARALLELISM));
    System.out.format("Input Parallelism is %d\n",parallelism);
    if (rateLimit > 0) {
      var kafkaConsumer = new RateLimitingKafkaConsumer<>(topic, schema, localProps,
          rateLimit);
      stream = GraphStream.fromSource(kafkaConsumer, config, parallelism);
    } else {
      stream = GraphStream
          .fromSource(new FlinkKafkaConsumer<>(topic, schema, localProps), config, parallelism);
    }
  }

  private int getNumberOfPartitions(Properties localProps) {
    var tempConsumer  = new org.apache.kafka.clients.consumer.KafkaConsumer<>(localProps);
    var partitions = tempConsumer.partitionsFor(properties.getProperty(TOPIC_KEY));
    return partitions.size();
  }

  private Properties createKafkaProperties(String bootstrapServerConfig) {
    var props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerConfig);
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "CitibikeConsumer" + Math.random());
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        TripletDeserializer.class.getName());
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10_000);
    return props;
  }

  public abstract AbstractStream<?> applyOperator(GraphStream stream);
}
