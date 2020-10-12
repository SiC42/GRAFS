package edu.leipzig.grafs.setup.writer;

import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.Vertex;
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
import org.gradoop.common.model.impl.id.GradoopId;

public class VertexAndEdgesToOneEdgeContainerFileWriter {

  private static final String BASE_SIZE = "100";

  private static final String VERTICE_PATH =
      "resources/2018-citibike-csv-" + BASE_SIZE + "/vertices.csv";
  private static final String EDGE_PATH = "resources/2018-citibike-csv-" + BASE_SIZE + "/edges.csv";

  private static final String EDGECONTAINER_FILE_NAME =
      "resources/edgecontainer_" + BASE_SIZE + ".serialized";

  public static void main(String[] args) {

    Map<GradoopId, Vertex> vertexMap = new HashMap<>();
    try (var pathsStream = Files.walk(Paths.get(VERTICE_PATH))) {
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
    try (var fileOutputStream = new FileOutputStream(EDGECONTAINER_FILE_NAME)) {
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
                  var ec = new EdgeContainer(edge, source, target);
                  oos.writeObject(ec);
                }
                System.out.print("Finished file '" + file.toString() + "'.");
                numberOfEdges.addAndGet(i);
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("Final number of edges: " + numberOfEdges.get());
//
//    var vertexMap = new HashMap<GradoopId, Vertex>();
//    for
//    var VertexMap = new VertexReader(vertexStream);

  }


}
