package edu.leipzig.grafs.setup;

import java.util.Properties;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public abstract class AbstractCmdBase {

  public static final String BASE_PATH = "base";
  public static final String TOPIC_KEY = "topic";

  private static final String CMD_INPUT = "fileinput";
  private static final String CMD_HELP = "help";
  private static final String CMD_CONFIG = "config";
  protected Properties properties;

  public AbstractCmdBase(String[] args) {
    properties = new Properties();
    checkArgs(args);
  }


  private void checkArgs(String[] args) {
    var parser = new DefaultParser();
    var options = buildOptions();
    var header = "Setup data for Benchmarking GRAFS";
    HelpFormatter formatter = new HelpFormatter();
    try {
      var cmd = parser.parse(options, args);
      if (cmd.hasOption(CMD_HELP)) {
        formatter.printHelp("grafs-data-setup", header, options, "");
        return;
      }
      if (cmd.hasOption(CMD_CONFIG)) {
        properties.putAll(edu.leipzig.grafs.setup.config.ProducerConfig
            .loadProperties(cmd.getOptionValue(CMD_CONFIG)));
      } else {
        properties.putAll(edu.leipzig.grafs.setup.config.ProducerConfig.loadDefaultProperties());
      }
      if (cmd.hasOption(CMD_INPUT)) {
        // do fileinput
        properties.put(BASE_PATH, cmd.getOptionValue(CMD_INPUT));
      } else {
        throw new ParseException(
            "Missing input. Either declare a fileinput or provide the information to a kafka server");
      }
      System.out.println("Loaded Properties:");
      System.out.println(properties);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("grafs-data-setup", header, options, "");

      System.exit(1);
    } catch (Exception e) {
      throw new RuntimeException("Could not find files");
    }
  }

  protected Options buildOptions() {
    var options = new Options();
    options.addOption("?", CMD_HELP, false, "print this message");
    options.addOption("i", CMD_INPUT, true, "base path to the input files");
    options.addOption("c", CMD_CONFIG, true, "specifies the location for the config data");
    return options;
  }

  public abstract void run();
}
