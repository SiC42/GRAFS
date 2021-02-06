package edu.leipzig.grafs.setup.reader;

import edu.leipzig.grafs.factory.VertexFactory;
import edu.leipzig.grafs.model.Vertex;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;

public class VertexReader extends CSVElementReader<String> {

  public VertexReader(InputStream csvStream) throws FileNotFoundException {
    super(csvStream);
  }

  @Override
  String parseGraphElem(String[] data) {
    var sb = new StringBuilder();
    try {
      sb.append(data[0]);
      sb.append(";");
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          e.getMessage() + " in id of element " + Arrays.toString(data));
    }
    try {
      sb.append(data[1]);
      sb.append(";");
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          e.getMessage() + " in graphIds of element " + Arrays.toString(data));
    }
    // label
    sb.append(data[2]);
    sb.append(";");

    // properties
    sb.append(data[3]);

    return sb.toString();
  }

  private Properties getProperties(String propertiesStr) {
    String[] propertyStrArray = propertiesStr.split("\\|");
    var properties = Properties.create();
    try {
      if (!propertyStrArray[0].equals("")) {
        properties.set("capacity", Integer.parseInt(propertyStrArray[0]));
      }
      properties.set("id", propertyStrArray[1]);
      properties.set("lat", propertyStrArray[2]);
      properties.set("long", propertyStrArray[3]);
      properties.set("name", propertyStrArray[4]);
      if (propertyStrArray.length > 5 && !propertyStrArray[5].equals("")) {
        properties.set("regionId", Integer.parseInt(propertyStrArray[5]));
      }
      if (propertyStrArray.length > 6) {
        properties.set("rentalURL", propertyStrArray[6]);
      }
    } catch (IndexOutOfBoundsException e){
      System.out.println(propertiesStr);
      throw e;
    }
    return properties;
  }

  public Map<String, String> getVertices() throws IOException {
    Map<String, String> graphElems = new HashMap<>();
    String row;
    while ((row = csvReader.readLine()) != null) {
      String[] data = row.split(DELIMITER);
      var graphElem = parseGraphElem(data);
      graphElems.put(data[0], graphElem);
    }
    csvReader.close();
    return graphElems;
  }
}
