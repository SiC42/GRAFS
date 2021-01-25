package edu.leipzig.grafs.benchmark.tests.window;

import edu.leipzig.grafs.benchmark.tests.AbstractBenchmark;
import edu.leipzig.grafs.model.streaming.nonwindow.AbstractNonWindowedStream;
import edu.leipzig.grafs.model.streaming.nonwindow.GraphStream;
import edu.leipzig.grafs.model.streaming.window.WindowedGraphStream;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

public abstract class AbstractWindowBenchmark extends AbstractBenchmark {

  Time windowSize;
  WindowAssigner<Object, TimeWindow> window;
  boolean useTrigger;
  Trigger<Object, TimeWindow> countTrigger;
  private int windowSizeInMs;
  private int countTriggerSize;

  public AbstractWindowBenchmark(String[] args) {
    super(args);
    windowSizeInMs = -1;
    countTriggerSize = -1;
    checkArgs(args);
  }

  private void checkArgs(String[] args) {
    var parser = new DefaultParser();
    var options = buildOptions();
    var header = String.format("Benchmarking GRAFS with %s.", operatorName);
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
      if (cmd.hasOption("triggersize")) {
        try {
          countTriggerSize = Integer.parseInt(cmd.getOptionValue("triggersize"));
        } catch (NumberFormatException e) {
          throw new ParseException("Error. argument after triggersize is not an integer.");
        }
        this.useTrigger = true;
        this.windowSize = Time.days(5); // obsolete, as the trigger will fire instead
        var windowedStream = ((GraphStream) stream)
            .window(TumblingProcessingTimeWindows.of(windowSize));
        windowedStream.trigger(PurgingTrigger.of(CountTrigger.of(countTriggerSize)));
        stream = windowedStream;
      }
    } catch (ParseException e) {
      e.printStackTrace();
      formatter.printHelp("grafsbenchmark", header, options, "");

      System.exit(1);
    }
  }

  protected String getCsvLine(long timeInMilliSeconds) {
    return String
        .format("%s;%d;%d;%d\n", this.operatorName, this.windowSizeInMs, this.countTriggerSize,
            timeInMilliSeconds);
  }

  protected Options buildOptions() {
    var options = super.buildOptions();
    options.addOption("ws", "windowsize", true, "size of the window in ms");
    options.addOption("ts", "triggersize", true, "number of elements trigger closing window");
    return options;
  }


  public AbstractNonWindowedStream applyOperator(GraphStream stream) {
    var windowedStream = stream.window(window);
    if (useTrigger) {
      windowedStream = windowedStream.trigger(countTrigger);
    }
    return applyOperator(windowedStream);
  }

  public abstract <W extends Window> AbstractNonWindowedStream applyOperator(
      WindowedGraphStream<W> stream);
}
