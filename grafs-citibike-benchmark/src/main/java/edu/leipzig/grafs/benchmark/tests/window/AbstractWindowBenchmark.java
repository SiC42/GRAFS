package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.streaming.AbstractStream;
import edu.leipzig.grafs.model.streaming.GraphStream;
import edu.leipzig.grafs.model.window.TumblingProcessingTimeWindows;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public abstract class AbstractWindowBenchmark extends AbstractBenchmark {

  Time windowSize;
  TumblingProcessingTimeWindows window;
  boolean useTrigger;
  Trigger<Object, TimeWindow> countTrigger;
  private int windowSizeInMs;

  public AbstractWindowBenchmark(String[] args) {
    super(args);
    windowSizeInMs = -1;
    checkArgs(args);
  }

  private void checkArgs(String[] args) {
    var parser = new DefaultParser();
    var options = buildOptions();
    var header = String
        .format("Benchmarking GRAFS with %s.", properties.getProperty(OPERATOR_NAME_KEY));
    HelpFormatter formatter = new HelpFormatter();
    try {
      var cmd = parser.parse(options, args);
      if (!cmd.hasOption("windowsize") && !cmd.hasOption("triggersize")) {
        throw new ParseException("Error. You have to set either 'windowsize' or 'triggersize'");
      }
      if (cmd.hasOption("windowsize") && cmd.hasOption("triggersize")) {
        throw new ParseException("Error. You cannot set 'windowsize' and 'triggersize'");
      }
      if (cmd.hasOption("windowsize")) {
        try {
          this.useTrigger = false;
          windowSizeInMs = Integer.parseInt(cmd.getOptionValue("windowsize"));
          this.windowSize = Time.milliseconds(windowSizeInMs);
          this.window = TumblingProcessingTimeWindows.of(windowSize);
        } catch (NumberFormatException e) {
          throw new ParseException("Error. argument after windowsize is not an integer.");
        }
      }
//      if (cmd.hasOption("triggersize")) {
//        try {
//          countTriggerSize = Integer.parseInt(cmd.getOptionValue("triggersize"));
//        } catch (NumberFormatException e) {
//          throw new ParseException("Error. argument after triggersize is not an integer.");
//        }
//        this.useTrigger = true;
//        this.windowSize = Time.days(5); // obsolete, as the trigger will fire instead
//        var windowedStream = ((GraphStream) stream)
//            .window(TumblingProcessingTimeWindows.of(windowSize));
//        windowedStream.trigger(PurgingTrigger.of(CountTrigger.of(countTriggerSize)));
//        stream = windowedStream;
//      }
    } catch (ParseException e) {
      e.printStackTrace();
      formatter.printHelp("grafsbenchmark", header, options, "");

      System.exit(1);
    }
  }

  protected String getCsvLine(long timeInMilliSeconds) {
    return getCsvLine(timeInMilliSeconds, windowSizeInMs);
  }

  protected Options buildOptions() {
    var options = super.buildOptions();
    options.addOption("ws", "windowsize", true, "size of the window in ms");
    options.addOption("ts", "triggersize", true, "number of elements trigger closing window");
    return options;
  }


  public AbstractStream<?> applyOperator(GraphStream stream) {
    return applyOperatorWithWindow(stream);
    //    if (useTrigger) {
//      windowedStream = windowedStream.trigger(countTrigger);
//    }
  }

  public abstract AbstractStream<?> applyOperatorWithWindow(GraphStream stream);
}
