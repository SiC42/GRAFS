package edu.leipzig.grafs.benchmark.tests.fixed;

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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public abstract class AbstractFixedSizeBenchmark {

  public static final String CMD_TOPIC = "topic";
  public static final String OPERATOR_NAME_KEY = "operatorname";
  public static final String OUTPUT_PATH_KEY = "output";

  private static final String CMD_KAFKA = "kafka";
  private static final String CMD_INPUT_PARALLELISM = "inputp";
  private static final String CMD_CONFIG = "config";
  private static final String CMD_PARALLELISM = "parallelism";
  private static final String CMD_RESULT = "result";
  private static final String CMD_WINDOW_SIZE = "windowsize";
  private static final String CMD_ITERATIONS = "iterations";

  private static final String CONFIG_RESULT_KEY = "result.single";

  private static final int MAX_PARALLELISM = 96;

  protected StreamExecutionEnvironment env;
  protected AbstractStream<?> stream;
  protected Writer outputWriter;
  protected Properties properties;

  private int windowSize;
  private int iterations;

  // TODO: Base this benchmark on AbstractBenchmark
  public AbstractFixedSizeBenchmark(String[] args) {
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

      // =================== Load Topic ====================
      if (cmd.hasOption(CMD_TOPIC)) {
        properties.put(CMD_TOPIC, cmd.getOptionValue(CMD_TOPIC));
      }

      if (cmd.hasOption(CMD_ITERATIONS)) {
        try {
          iterations = Integer.parseInt(cmd.getOptionValue(CMD_ITERATIONS));
        } catch (NumberFormatException e) {
          throw new ParseException("Error. argument after iterations is not an integer.");
        }
      }

      if (cmd.hasOption(CMD_WINDOW_SIZE)) {
        try {
          windowSize = Integer.parseInt(cmd.getOptionValue(CMD_WINDOW_SIZE));
        } catch (NumberFormatException e) {
          throw new ParseException("Error. argument after windowsize is not an integer.");
        }
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

      // ================= Process OUTPUT ==================
      if (cmd.hasOption("output")) {
        properties.put(OUTPUT_PATH_KEY,
            cmd.getOptionValue("output") + "output_" + System.currentTimeMillis());
      }

      // =============== Process RESULT LOG ================
      String logPath;
      if (cmd.hasOption(CMD_RESULT)) {
        properties.put(CONFIG_RESULT_KEY, cmd.getOptionValue(CMD_RESULT));
      }

      logPath = properties.getProperty(CONFIG_RESULT_KEY);

      try {
        var fileOutputStream = new FileOutputStream(logPath, true);
        this.outputWriter = new OutputStreamWriter(fileOutputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new ParseException("Unreadable output path");
      }

    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("grafs-benchmark", header, options, "");
      System.exit(1);
    }

  }

  public void execute() throws Exception {
    var consumer = CitibikeConsumer.createConsumer(properties);

    Iterator<ConsumerRecord<String, String>> recordIt = null;
    var resultList = new ArrayList<Long>();
    var mapper = new StringToTripletMapper();

    System.out.println("Starting to read from Kafka.");
    for (int i = 0; i < iterations; i++) {
      System.out.println("Iteration: " + i);
      var tripletCol = new ArrayList<Triplet>();
      while (tripletCol.size() < windowSize) {
        if (recordIt == null || !recordIt.hasNext()) {
          var consumerRecords = consumer.poll(Duration.ofMillis(1000));
          recordIt = consumerRecords.iterator();
        }
        while (tripletCol.size() < windowSize && recordIt.hasNext()) {
          var nextTriplet = mapper.map(recordIt.next().value());
          if (filter(nextTriplet)) {
            tripletCol.add(nextTriplet);
          }
        }
      }

      var stream = env.fromCollection(tripletCol);
      var config = new FlinkConfigBuilder(env)
          .withWaterMarkStrategy(WatermarkStrategy
              .<Triplet>forBoundedOutOfOrderness(Duration.ZERO)
              .withTimestampAssigner((t, timestamp) -> 0))
          .build();
      var graphStream = new GraphStream(stream, config);
      var finalStream = applyOperator(graphStream);
      finalStream.addSink(new DiscardingSink<>());
      var result = env.execute(properties.getProperty(OPERATOR_NAME_KEY));
      var timeInMilliSeconds = result.getNetRuntime(TimeUnit.MILLISECONDS);
      resultList.add(timeInMilliSeconds);
    }
    outputWriter.write(getCsvLine(resultList, windowSize));
    outputWriter.flush();
    outputWriter.close();

  }

  protected String getCsvLine(Collection<Long> times, int windowSize) {
    var outputTypeStr = properties.contains(OUTPUT_PATH_KEY) ? "w" : "d";
    var parallelism = properties.getProperty(CMD_PARALLELISM, "-1");
    var sb = new StringBuilder();
    for (var time : times) {
      sb.append(";");
      sb.append(time);
    }
    return String
        .format("%s;%s;%s;%d%s\n",
            properties.getProperty(OPERATOR_NAME_KEY),
            outputTypeStr,
            parallelism,
            windowSize,
            sb.toString());
  }

  protected Options buildOptions() {
    var options = new Options();
    options.addOption("h", "help", false, "print this message");
    options.addOption("kip", CMD_KAFKA, true, "the kafka server in the format hostname:port/topic");
    options.addOption("o", "output", true, "location for the output file");
    options.addOption(CMD_RESULT, true, "location for the result file");
    options.addOption("c", CMD_CONFIG, true, "location for the config file");
    options.addOption("t", CMD_TOPIC, true, "get topic for the kafka consumer");
    options.addOption("ws", CMD_WINDOW_SIZE, true, "size of the window (# of elements)");
    options.addOption("i", CMD_ITERATIONS, true, "# of iterations before testing stops");
    options.addOption("p", CMD_PARALLELISM, true, "parallelism set in execution environment");
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

  protected boolean filter(Triplet triplet) {
    return true;
  }
}
