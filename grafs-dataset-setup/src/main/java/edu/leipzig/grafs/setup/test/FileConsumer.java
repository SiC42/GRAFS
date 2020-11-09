package edu.leipzig.grafs.setup.test;


import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.serialization.EdgeContainerDeserializationSchema;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class FileConsumer {

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    var fileOutputStream = new FileInputStream("resources/edgecontainer_10.serialized");
    var ois = new ObjectInputStream(fileOutputStream);
    var ec = (EdgeContainer) ois.readObject();
    int counter = 1;
    var queue = new SmallQueue();
    while (!ec.getEdge().getLabel()
        .equals(EdgeContainerDeserializationSchema.END_OF_STREAM_LABEL)) {
      queue.add(ec);
      counter++;
      if(counter % 10000 == 0){
        System.out.print(counter + "\r");
        System.out.flush();
      }
      ec = (EdgeContainer) ois.readObject();
    }
    queue.add(ec);
    queue.asColl().forEach(System.out::println);
  }

}
