package edu.leipzig.grafs.setup;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public abstract class AbstractCmdBase {

  private static final String INPUT = "fileinput";
  private static final String HELP = "help";
  protected String BASE_PATH;

  public AbstractCmdBase(String[] args) {
    checkArgs(args);
  }


  private void checkArgs(String[] args) {
    var parser = new DefaultParser();
    var options = buildOptions();
    var header = "Setup data for Benchmarking GRAFS";
    HelpFormatter formatter = new HelpFormatter();
    try {
      var cmd = parser.parse(options, args);
      if (cmd.hasOption(HELP)) {
        formatter.printHelp("grafs-data-setup", header, options, "");
        return;
      }
      if (cmd.hasOption(INPUT)) {
        // do fileinput
        BASE_PATH = cmd.getOptionValue(INPUT);
      } else {
        throw new ParseException(
            "Missing input. Either declare a fileinput or provide the information to a kafka server");
      }
    } catch (ParseException e) {
      formatter.printHelp("grafs-data-setup", header, options, "");

      System.exit(1);
    }
  }

  protected Options buildOptions() {
    var options = new Options();
    options.addOption("?", HELP, false, "print this message");
    options.addOption("i", INPUT, true, "path to the input files");
    options.addOption("o", "output", true, "location for the output file");
    return options;
  }

  public abstract void run();
}
