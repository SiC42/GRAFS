package edu.leipzig.grafs.benchmark.operators.union;

import edu.leipzig.grafs.benchmark.operators.functions.SimpleMeter;
import edu.leipzig.grafs.model.Triplet;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.operators.union.DisjunctUnion;
import org.apache.flink.streaming.api.datastream.DataStream;

public class BenchmarkDisjunctUnion extends DisjunctUnion {


  private final String meterName;

  public BenchmarkDisjunctUnion(EdgeStream... streams) {
    this("disjunctUnionMeter", streams);
  }

  public BenchmarkDisjunctUnion(String meterName, EdgeStream... streams) {
    super(streams);
    this.meterName = meterName;
  }

  @Override
  public DataStream<Triplet> execute(DataStream<Triplet> stream) {
    return super.execute(stream).map(new SimpleMeter<>(meterName));
  }
}
