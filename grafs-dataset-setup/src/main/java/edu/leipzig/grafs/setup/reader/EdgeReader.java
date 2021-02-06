package edu.leipzig.grafs.setup.reader;

import edu.leipzig.grafs.factory.EdgeFactory;
import edu.leipzig.grafs.model.Edge;
import edu.leipzig.grafs.model.Vertex;
import edu.leipzig.grafs.setup.model.CSVEdge;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

public class EdgeReader extends CSVElementReader<CSVEdge> {

  public EdgeReader(InputStream csvStream) throws FileNotFoundException {
    super(csvStream);
  }

  CSVEdge parseGraphElem(String[] data) {

    var sb = new StringBuilder();
    //id
    sb.append(data[0]);
    sb.append(";");

    // graph ids
    sb.append(data[1]);
    sb.append(";");

    var sourceId = data[2];
    sb.append(sourceId);
    sb.append(";");
    var targetId = data[3];
    sb.append(targetId);
    sb.append(";");

    // label
    sb.append(data[4]);
    sb.append(";");

    sb.append(data[5]);

    return new CSVEdge(data[0],sb.toString(), sourceId, targetId);
  }

  private Properties getProperties(String propertiesStr) {
    String[] propertyStrArray = propertiesStr.split("\\|");
    var properties = Properties.create();
    if (propertyStrArray.length == 7) {
      properties.set("bike_id", propertyStrArray[0]);
      properties.set("gender", propertyStrArray[1]);
      properties.set("starttime", propertyStrArray[2]);
      properties.set("stoptime", propertyStrArray[3]);
      properties.set("tripduration", propertyStrArray[4]);
      properties.set("user_type", propertyStrArray[5]);
      properties.set("year_birth", propertyStrArray[6]);
    }
    return properties;
  }

  public CSVEdge getEdge() throws IOException {
    Map<GradoopId, Vertex> graphElems = new HashMap<>();
    String row;
    if ((row = csvReader.readLine()) != null) {
      String[] data = row.split(DELIMITER);
      var edge = parseGraphElem(data);
      return edge;
    } else {
      csvReader.close();
      return null;
    }
  }
}
