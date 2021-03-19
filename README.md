# GRAFS - A Distributed End-to-End Graph Stream Analytic System
GRAFS (**GR**aph **A**nalytics with Apache **F**link's **S**tream-API) is a prototype for scalable graph stream analytics using Apache Flink's DataStream-API, which is part of my Master Thesis.
It's graph model is adapted from the Property Graph Model and the [Extended Property Graph Model](https://dbs.uni-leipzig.de/file/EPGM.pdf) introduced by University of Leipzig.
An element in the stream, called *Triplet*, contains exactly one edge of the graph and its source and target vertex.
Each Graph element can hold a label and multiple properties, as well as its graph memberships. 


## Usage
### grafs-impl
This module contains the implementation of GRAFS.
It features 4 operators, namly *Subgraphing*, *Transformation*, *Windowed Grouping* and *Windowed Dual Simulation*, as well as the auxilary operator *Reduce*.
An introduction to each operator can be found in the [Gradoop wiki](https://github.com/dbs-leipzig/gradoop/wiki/List-of-Operators).
For detailed explanation of those read the master thesis.

*GraphStream* and *GCStream* are the main classes.
Simply convert your graph stream source to a Flink DataStream using *Triplet*s and use that as parameter for the stream.
An example for an analytics pipeline can be found below
```Java
// define config before with the streaming environment used for the DataStream
GraphStream gs = new GraphStream(stream, config);
// Let's assume this is a stream of persons (vertices) and its relationsships with each other.
// Mark everyone as adult or child and find only relationships between two adults or two children
GCStream gcs = gs
    .transformVertices(v -> {
      v.getPropertyValue("age").getInt() > 18
        ? v.setProperty("adult", true)
        : v.setProperty("adult", false);
        return v;
        })
    .dualSimulation("MATCH (a)->(b)" + 
                    "WHERE a.adult = b.adult");
```

### grafs-citibike-benchmark
This module contains the benchmark test for GRAFS.

### grafs-data-setup
This module contains the data setup to upload the csv-files as triplets to a Apache Kafka cluster. 