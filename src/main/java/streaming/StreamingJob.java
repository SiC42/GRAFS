/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming;

import java.util.Collection;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import streaming.model.EdgeContainer;
import streaming.model.EdgeStream;
import streaming.util.AsciiGraphLoader;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();
    String graphStr = "g1:graph[" +
        "(p1:Person {name: \"Bob\", age: 24})-[:friendsWith]->" +
        "(p2:Person{name: \"Alice\", age: 30})-[:friendsWith]->(p1)" +
        "(p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27})-[:friendsWith]->(p2) " +
        "(p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40})-[:friendsWith]->(p3) " +
        "(p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33})-[:friendsWith]->(p4) " +
        "(c1:Company {name: \"Acme Corp\"}) " +
        "(c2:Company {name: \"Globex Inc.\"}) " +
        "(p2)-[:worksAt]->(c1) " +
        "(p4)-[:worksAt]->(c1) " +
        "(p5)-[:worksAt]->(c1) " +
        "(p1)-[:worksAt]->(c2) " +
        "(p3)-[:worksAt]->(c2) " + "] " +
        "g2:graph[" +
        "(p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37})-[:friendsWith]->(p4) " +
        "(p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23})-[:friendsWith]->(p6) " +
        "(p8:Person {name: \"Jil\", age: 32})-[:friendsWith]->(p7)-[:friendsWith]->(p8) " +
        "(p6)-[:worksAt]->(c2) " +
        "(p7)-[:worksAt]->(c2) " +
        "(p8)-[:worksAt]->(c1) " + "]";
    AsciiGraphLoader loader = AsciiGraphLoader.fromString(graphStr);
    Collection<EdgeContainer> ecCollection = loader.createEdgeContainers();
    DataStream<EdgeContainer> m = env.fromCollection(ecCollection);

    EdgeStream messageStream = new EdgeStream(m);
//        EdgeStream filteredStream = messageStream.filter(e -> e.getFrom().getProperty("id").equals("A"))
//                .transform(e -> {
//                    String content = e.get("id").replaceAll("Fuck", "F***");
//                    e.put("id", content);
//                    return e;
//                }).transformVertices(v ->
//                {
//                    if (v.getProperty("id").equals("A")) {
//                        v.put("id", "Albert");
//                    } else {
//                        v.put("id", "Bernd");
//                    }
//                    return v;
//                });

    messageStream.print();

    env.execute();
  }

  public static void gradoop() {
    String graph = "g1:graph[" +
        "(p1:Person {name: \"Bob\", age: 24})-[:friendsWith]->" +
        "(p2:Person{name: \"Alice\", age: 30})-[:friendsWith]->(p1)" +
        "(p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27})-[:friendsWith]->(p2) " +
        "(p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40})-[:friendsWith]->(p3) " +
        "(p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33})-[:friendsWith]->(p4) " +
        "(c1:Company {name: \"Acme Corp\"}) " +
        "(c2:Company {name: \"Globex Inc.\"}) " +
        "(p2)-[:worksAt]->(c1) " +
        "(p4)-[:worksAt]->(c1) " +
        "(p5)-[:worksAt]->(c1) " +
        "(p1)-[:worksAt]->(c2) " +
        "(p3)-[:worksAt]->(c2) " + "] " +
        "g2:graph[" +
        "(p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37})-[:friendsWith]->(p4) " +
        "(p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23})-[:friendsWith]->(p6) " +
        "(p8:Person {name: \"Jil\", age: 32})-[:friendsWith]->(p7)-[:friendsWith]->(p8) " +
        "(p6)-[:worksAt]->(c2) " +
        "(p7)-[:worksAt]->(c2) " +
        "(p8)-[:worksAt]->(c1) " + "]";

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(cfg);
    loader.initDatabaseFromString(graph);
    LogicalGraph n1 = loader.getLogicalGraphByVariable("g1");
    LogicalGraph n2 = loader.getLogicalGraphByVariable("g2");
  }
}
