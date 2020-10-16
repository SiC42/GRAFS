package edu.leipzig.grafs.benchmark;

import edu.leipzig.grafs.benchmark.operators.transform.BenchmarkVertexTransformation;
import edu.leipzig.grafs.connectors.RateLimitingKafkaConsumer;
import edu.leipzig.grafs.model.EdgeContainer;
import edu.leipzig.grafs.model.EdgeStream;
import edu.leipzig.grafs.serialization.EdgeContainerDeserializationSchema;
import edu.leipzig.grafs.util.FlinkConfigBuilder;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MeterBenchmark {


  public static void main(String[] args) throws Exception {
    var es = createEdgeStream();
    es = es.callForStream(new BenchmarkVertexTransformation(v -> {
      v.setProperty("enteredStream", true);
      return v;
    }));
    var test = es.collect();
    var i = 0;
    while (test.hasNext()){
      var item = test.next();
      i++;
      if(i % 1000 == 0){
        System.out.println("in iterator");
        System.out.println(item);
      }
    }
    System.out.println("END OF THE STREAM. ITEMS:");
    System.out.println(i);
  }

  private static EdgeStream createEdgeStream() {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    var config = new FlinkConfigBuilder(env)
        .withWaterMarkStrategy(WatermarkStrategy
            .<EdgeContainer>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((ec, timestamp) -> 0))
        .build();
    var schema = new EdgeContainerDeserializationSchema();
    var kafkaConsumer = new RateLimitingKafkaConsumer<>("citibike", schema, CitibikeConsumer
        .createProperties(new Properties()), 100);
    kafkaConsumer.setStartFromEarliest();

    return EdgeStream.fromSource(kafkaConsumer, config);
  }
}
