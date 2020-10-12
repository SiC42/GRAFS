package edu.leipzig.grafs.setup.reader;

import edu.leipzig.grafs.model.EdgeContainer;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class SerializedEdgeContainerFileReader implements Closeable {

  private static final String BASE_SIZE = "100";

  private static final String EDGECONTAINER_FILE_NAME =
      "resources/edgecontainer_" + BASE_SIZE + ".serialized";

  private final ObjectInputStream ois;

  private EdgeContainer nextEc;

  private boolean hasNext;

  private final long lineCount;

  public SerializedEdgeContainerFileReader() throws IOException, ClassNotFoundException {
    this(EDGECONTAINER_FILE_NAME);
  }

  public SerializedEdgeContainerFileReader(String fileStr)
      throws IOException, ClassNotFoundException {
    lineCount = Files.lines(Paths.get(fileStr)).count();
    var fileOutputStream = new FileInputStream(fileStr);
    this.ois = new ObjectInputStream(fileOutputStream);
    nextEc = (EdgeContainer) ois.readObject();
    hasNext = nextEc != null;
  }

  public EdgeContainer getNext() throws IOException, ClassNotFoundException {
    var lastEc = nextEc;
    if ((nextEc = (EdgeContainer) ois.readObject()) == null) {
      hasNext = false;
    }
    return lastEc;
  }

  public boolean hasNext() {
    return hasNext;
  }

  public long getNumberOfLines() {
    return lineCount;
  }

  @Override
  public void close() throws IOException {
    ois.close();
  }
}
