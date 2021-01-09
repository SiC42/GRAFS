package edu.leipzig.grafs.setup.writer;


import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.serialization.TripletDeserializationSchema;
import edu.leipzig.grafs.setup.AbstractCmdBase;
import edu.leipzig.grafs.setup.reader.EdgeReader;
import edu.leipzig.grafs.setup.reader.VertexReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.gradoop.common.model.impl.id.GradoopId;


/**
 * Combines the csv-files of a graph into one file with serialized triplets.
 */
public class VertexAndEdgesToTripleFileWriter extends AbstractCmdBase {


  private final String VERTEX_PATH = BASE_PATH + "/vertices.csv";
  private final String EDGE_PATH = BASE_PATH + "/edges.csv";
  private final String OUTPUT = "output";
  private String outputPath;

  public VertexAndEdgesToTripleFileWriter(String[] args) {
    super(args);
    checkArgs(args);
  }

  public static void main(String[] args) {
    var writer = new VertexAndEdgesToTripleFileWriter(args);
    writer.run();
  }

  private void checkArgs(String[] args) {
    var parser = new DefaultParser();
    var options = buildOptions();
    var header = "Setup data for Benchmarking GRAFS";
    HelpFormatter formatter = new HelpFormatter();
    try {
      var cmd = parser.parse(options, args);
      if (cmd.hasOption(OUTPUT)) {
        this.outputPath = cmd.getOptionValue(OUTPUT);
      } else {
        throw new ParseException("Missing parameter: o");
      }
    } catch (ParseException e) {
      formatter.printHelp("grafs-data-setup", header, options, "");

      System.exit(1);
    }
  }

  protected Options buildOptions() {
    var options = super.buildOptions();
    options.addOption("o", OUTPUT, true, "path and name of the output file");
    return options;
  }

  public void run() {

    Map<GradoopId, Vertex> vertexMap = new HashMap<>();
    try (var pathsStream = Files.walk(Paths.get(VERTEX_PATH))) {
      pathsStream.filter(Files::isRegularFile)
          .forEach(file -> {
            System.out.print("Processing file '" + file.toString() + "'\r");
            System.out.flush();
            try {
              var inStream = Files.newInputStream(file);
              var vertexReader = new VertexReader(inStream);
              vertexMap.putAll(vertexReader.getVertices());
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("Finished processing vertices");
    System.out.println("Found " + vertexMap.size() + " vertices.");
    AtomicInteger numberOfEdges = new AtomicInteger();
    try (var fileOutputStream = new FileOutputStream(outputPath)) {
      ObjectOutputStream oos = new ObjectOutputStream(fileOutputStream);
      try (var pathsStream = Files.walk(Paths.get(EDGE_PATH))) {
        pathsStream.filter(Files::isRegularFile)
            .forEach(file -> {
              try (var inStream = Files.newInputStream(file)) {
                final var curFileMessageStr = "Current file: '" + file.toString() + "'\t";
                int i = 0;
                long lineCount = Files.lines(file).count();
                var edgeReader = new EdgeReader(inStream);
                Edge edge;
                double curLine;
                while ((edge = edgeReader.getNextEdge()) != null) {
                  curLine = ++i;
                  if (curLine % 5000 == 0) {
                    var relativeProgress = Math.round(curLine * 100 / lineCount);
                    System.out.print(curFileMessageStr + " Progress: " + relativeProgress + "%\r");
                    System.out.flush();
                  }
                  if (curLine % 100_000 == 0) {
                    oos.reset();
                  }
                  var source = vertexMap.get(edge.getSourceId());
                  var target = vertexMap.get(edge.getTargetId());
                  var triplet = new Triplet(edge, source, target);
                  oos.writeObject(triplet);
                }

                System.out.print("Finished file '" + file.toString() + "'.\n");
                System.out.flush();
                numberOfEdges.addAndGet(i);
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
        // send a last object that is not part of the analysis, but marks end of stream
        var source = new Vertex();
        var END_OF_STREAM_LABEL = TripletDeserializationSchema.END_OF_STREAM_LABEL;
        source.setLabel(END_OF_STREAM_LABEL);
        var target = new Vertex();
        target.setLabel(END_OF_STREAM_LABEL);
        var edge = EdgeFactory.createEdge(source, target);
        edge.setLabel(END_OF_STREAM_LABEL);
        oos.writeObject(new Triplet(edge, source, target));
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("Final number of edges: " + numberOfEdges.get());
  }


}
