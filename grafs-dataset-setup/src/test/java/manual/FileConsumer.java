package manual;

import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.serialization.SimpleStringSchemaWithEnd;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Simple tests class, reads the given serialized edge container file and prints the last 10 ECs.
 */
public class FileConsumer {

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    var fileOutputStream = new FileInputStream("resources/Triplet_10.serialized");
    var ois = new ObjectInputStream(fileOutputStream);
    var triplet = (Triplet) ois.readObject();
    int counter = 1;
    var queue = new SmallQueue();
    while (!triplet.getEdge().getLabel()
        .equals(SimpleStringSchemaWithEnd.END_OF_STREAM_LABEL)) {
      queue.add(triplet);
      counter++;
      if (counter % 10000 == 0) {
        System.out.print(counter + "\r");
        System.out.flush();
      }
      triplet = (Triplet) ois.readObject();
    }
    queue.add(triplet);
    queue.asColl().forEach(System.out::println);
  }

}
