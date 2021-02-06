package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.setup.model.CSVEdge;
import edu.leipzig.grafs.setup.reader.EdgeReader;
import edu.leipzig.grafs.setup.reader.VertexReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.gradoop.common.model.impl.id.GradoopId;

public class CsvToKafkaProducer extends AbstractProducer {


  private static final String VERTICE_PATH = "/vertices.csv";
  private static final String EDGE_PATH = "/edges.csv";

  public CsvToKafkaProducer(String[] args) {
    super(args);
  }

  public static void main(String... args) {
    var producer = new CsvToKafkaProducer(args);
    producer.run();
  }

  public void run() {
    long start = System.nanoTime();
    System.out.println("Starting reading files. Vertices first...");
    Map<String, String> vertexMap = new HashMap<>();
    var basePath = properties.getProperty(BASE_PATH);
    try (var pathsStream = Files.walk(Paths.get(basePath + VERTICE_PATH))) {
      pathsStream.filter(Files::isRegularFile)
          .forEach(file -> {
            System.out.print("Processing file '" + file.toString() + "'\r");
            System.out.flush();
            try(var inStream = Files.newInputStream(file)) {
              var vertexReader = new VertexReader(inStream);
              vertexMap.putAll(vertexReader.getVertices());
            } catch (IOException e) {
              e.printStackTrace();
            }finally {
              System.out.println();
            }
          });
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("Finished processing vertices.");
    System.out.println("Found " + vertexMap.size() + " vertices.");
    AtomicInteger numberOfEdges = new AtomicInteger();
    try (var pathsStream = Files.walk(Paths.get(basePath + EDGE_PATH))) {
      pathsStream.filter(Files::isRegularFile)
          .forEach(file -> {
            try (var inStream = Files.newInputStream(file)) {
              final var curFileMessageStr = "Current file: '" + file.toString() + "'\t";
              int i = 0;
              long lineCount = Files.lines(file).count();
              var edgeReader = new EdgeReader(inStream);
              CSVEdge edge;
              double curLine;
              while ((edge = edgeReader.getEdge()) != null) {
                curLine = ++i;
                if (curLine % 5000 == 0) {
                  var relativeProgress = Math.round(curLine * 100 / lineCount);
                  System.out.print(curFileMessageStr + " Progress: " + relativeProgress + "%\r");
                  System.out.flush();
                }
                var source = vertexMap.get(edge.sourceId);
                var target = vertexMap.get(edge.targetId);
                var ec = String.format("%s\t%s\t%s",source,edge,target);
                sendTriplet(edge.id,ec);
              }

              System.out.println("Finished file '" + file.toString() + "'.");
              numberOfEdges.addAndGet(i);
            } catch (IOException | InterruptedException | ExecutionException e) {
              e.printStackTrace();
            }
          });

      sendEndOfStreamToAllPartitions();
    } catch (IOException | InterruptedException | ExecutionException e) {
      e.printStackTrace();
    } finally {
      producer.flush();
      producer.close();
    }
    long end = System.nanoTime();
    System.out.format("Finished process after %d seconds.", (end - start) / 1_000_000_000);
    System.out.println("Final number of edges: " + numberOfEdges.get());
  }

}
