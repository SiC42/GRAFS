package edu.leipzig.grafs.setup.kafka;

import edu.leipzig.grafs.setup.reader.SerializedTripletFileReader;
public class FileToKafkaProducer extends AbstractProducer {


  public FileToKafkaProducer(String[] args) {
    super(args);
  }

  public static void main(String... args) {
    var producer = new FileToKafkaProducer(args);
    producer.run();
  }

  public void run() {
    try (var reader = new SerializedTripletFileReader(BASE_PATH)) {
      double curLine = 0;
      System.out.println("Starting reading elements.");
      while (reader.hasNext()) {
        curLine++;
        if (curLine % 10000 == 0) {
          System.out.println(curLine + " lines processed.");
        }
        var triplet = reader.getNext();
        sendTriplet(triplet);
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
