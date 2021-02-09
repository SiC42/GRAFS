package edu.leipzig.grafs.benchmark.tests.streaming;

import edu.leipzig.grafs.benchmark.CitibikeConsumer;
import edu.leipzig.grafs.benchmark.config.ProducerConfig;
import edu.leipzig.grafs.benchmark.connectors.RateLimitingKafkaConsumer;
import edu.leipzig.grafs.benchmark.serialization.SimpleStringSchemaWithEnd;
import edu.leipzig.grafs.benchmark.serialization.StringToTripletMapper;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

// TODO: Base this benchmark on AbstractBenchmark
public abstract class AbstractStreamingBenchmark {

  public static final String CMD_TOPIC = "topic";
  public static final String OPERATOR_NAME_KEY = "operatorname";
  public static final String OUTPUT_PATH_KEY = "output";

  private static final String CMD_KAFKA = "kafka";
  private static final String CMD_RATE_LIMIT = "ratelimit";
  private static final String CMD_INPUT_PARALLELISM = "inputp";
  private static final String CMD_CONFIG = "config";
  private static final String CMD_PARALLELISM = "parallelism";
  private static final String CMD_RESULT = "result";

  private static final int MAX_PARALLELISM = 96;

  protected StreamExecutionEnvironment env;
  protected GraphStream stream;
  protected Writer outputWriter;
  protected Properties properties;

  public AbstractStreamingBenchmark(String[] args) {
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

      // ============= Load Configuration file ==============
      if (cmd.hasOption(CMD_CONFIG)) {
        try {
          properties.putAll(ProducerConfig.loadProperties(cmd.getOptionValue(CMD_CONFIG)));
        } catch (IOException ioE) {
          System.err.println("Could not find provided config.");
        }
      } else {
        properties.putAll(ProducerConfig.loadDefaultProperties());
      }

      // ============= Load Configuration file ==============
      if (cmd.hasOption(CMD_TOPIC)) {
        properties.put(CMD_TOPIC, cmd.getOptionValue(CMD_TOPIC));
      }

      // ================= Input Parallelism =================
      int numOfPartitions = getNumberOfPartitions(
          CitibikeConsumer.createProperties(properties.getProperty("bootstrap.servers")));
      System.out.format("Found %d partitions.\n", numOfPartitions);
      if (cmd.hasOption(CMD_INPUT_PARALLELISM)) {
        try {
          var inputParallelism = Integer.parseInt(cmd.getOptionValue(CMD_INPUT_PARALLELISM));
          if (inputParallelism > numOfPartitions) {
            throw new ParseException("Provided number is greater than number of partitions");
          }
          properties.put(CMD_INPUT_PARALLELISM, String.valueOf(inputParallelism));
        } catch (NumberFormatException e) {
          throw new ParseException("Provided argument for input parallelism is not a number.");
        }
      } else {
        properties.put(CMD_INPUT_PARALLELISM, String.valueOf(numOfPartitions));
      }

      // ============== Processing Parallelism ==============
      int parallelism;
      if (cmd.hasOption(CMD_PARALLELISM)) {
        try {
          parallelism = Integer.parseInt(cmd.getOptionValue(CMD_PARALLELISM));
          if (parallelism < 1) {
            throw new NumberFormatException("Not a positive number");
          }
          properties.put(CMD_PARALLELISM, String.valueOf(parallelism));
        } catch (NumberFormatException e) {
          throw new ParseException("Provided argument for parallelism is not a valid number.");
        }
      }

      // ==================== Rate Limit ====================
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
      // ================== Build Stream ===================
      buildStreamWithKafkaConsumer(rateLimit);

      // ================= Process OUTPUT ==================
      if (cmd.hasOption("output")) {
        properties.put(OUTPUT_PATH_KEY,
            cmd.getOptionValue("output") + "output_" + System.currentTimeMillis());
      }

      // =============== Process RESULT LOG ================
      String logPath;
      if (cmd.hasOption(CMD_RESULT)) {
        properties.put(CMD_RESULT, cmd.getOptionValue(CMD_RESULT));
      }

      logPath = properties.getProperty(CMD_RESULT);

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
    var baseStream = applyOperator(stream);
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

  protected String getCsvLine(long timeInMilliSeconds) {
    return getCsvLine(timeInMilliSeconds, -1);
  }

  protected String getCsvLine(long timeInMilliSeconds, int windowSize) {
    var outputTypeStr = properties.contains(OUTPUT_PATH_KEY) ? "w" : "d";
    var parallelism = properties.getProperty(CMD_PARALLELISM, "-1");
    return String
        .format("%s;%s;%s;%s;%d;%d\n",
            properties.getProperty(OPERATOR_NAME_KEY),
            properties.getProperty(CMD_TOPIC),
            outputTypeStr,
            parallelism,
            windowSize,
            timeInMilliSeconds);
  }

  protected Options buildOptions() {
    var options = new Options();
    options.addOption("h", "help", false, "print this message");
    options.addOption("kip", CMD_KAFKA, true, "the kafka server in the format hostname:port/topic");
    options.addOption("i", CMD_INPUT_PARALLELISM, true,
        "maximum number of flink worker to read from source. Has to me smaller than the number of topic partitions on the kafka server");
    options.addOption("l", CMD_RATE_LIMIT, true,
        "the rate limit for the intake of data into the system");
    options.addOption("o", "output", true, "location for the output file");
    options.addOption(CMD_RESULT, true, "location for the result file");
    options.addOption("c", CMD_CONFIG, true, "location for the config file");
    options.addOption("p", CMD_PARALLELISM, true, "parallelism set in execution environment");
    options.addOption("t", CMD_TOPIC, true, "get topic for the kafka consumer");
    return options;
  }

  private void buildStreamWithKafkaConsumer(int rateLimit) {
    var localProps = CitibikeConsumer.createProperties(properties.getProperty("bootstrap.servers"));
    var schema = new SimpleStringSchemaWithEnd();
    var topic = properties.getProperty(CMD_TOPIC);
    if (properties.containsKey(CMD_PARALLELISM)) {
      var parallelism = Integer.parseInt(properties.getProperty(CMD_PARALLELISM));
      System.out.format("Processing Parallelism is %d\n", parallelism);
      env.setParallelism(parallelism);
    }

    // Prepare consumer
    FlinkKafkaConsumer<String> kafkaConsumer;
    String sourceName;
    if (rateLimit > 0) {
      kafkaConsumer = new RateLimitingKafkaConsumer<>(topic, schema, localProps, rateLimit);
      sourceName = "Rate-Limiting Kafka-Consumer";
    } else {
      kafkaConsumer = new FlinkKafkaConsumer<>(topic, schema, localProps);
      sourceName = "Kafka-Consumer";
    }
    int inputParallelism = Integer.parseInt(properties.getProperty(CMD_INPUT_PARALLELISM));
    System.out.format("Input Parallelism is %d\n", inputParallelism);
    DataStream<String> dataStream = env
        .addSource(kafkaConsumer)
        .name(sourceName)
        .setParallelism(inputParallelism);
    var config = new FlinkConfigBuilder(env).build();
    stream = new GraphStream(transformToTripletStream(dataStream, inputParallelism), config);
  }

  private DataStream<Triplet> transformToTripletStream(DataStream<String> stream, int parallelism) {
    return stream
        .map(new StringToTripletMapper())
        .name("Parse String to Triplet")
        .setParallelism(MAX_PARALLELISM);
  }

  private int getNumberOfPartitions(Properties localProps) {
    var tempConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(localProps);
    var partitions = tempConsumer.partitionsFor(properties.getProperty(CMD_TOPIC));
    return partitions.size();
  }

  public abstract AbstractStream<?> applyOperator(GraphStream stream);
}
