package edu.leipzig.grafs.setup.model;


public class CSVEdge {

  public final String id;
  public final String edgeCsvString;
  public final String sourceId;
  public final String targetId;

  public CSVEdge(String id, String edgeCsvString, String sourceId, String targetId) {
    this.id = id;
    this.edgeCsvString = edgeCsvString;
    this.sourceId = sourceId;
    this.targetId = targetId;
  }

  @Override
  public String toString() {
    return edgeCsvString;
  }
}
