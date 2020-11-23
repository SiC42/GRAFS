package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.setup.reader.SerializedEdgeContainerFileReader;

public class FileToKafkaProducer extends AbstractProducer {

  public static void main(String... args) {
    var producer = new FileToKafkaProducer();
    producer.run();
  }

  public void run() {
    try (var reader = new SerializedEdgeContainerFileReader()) {
      double curLine = 0;
      System.out.println("Starting reading elements.");
      while (reader.hasNext()) {
        curLine++;
        if (curLine % 10000 == 0) {
          System.out.println(curLine + " lines processed.");
        }
        var ec = reader.getNext();
        sendEdgeContainer(ec);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.flush();
      producer.close();
    }
    System.out.println("Finished reading elements.");
  }

}
