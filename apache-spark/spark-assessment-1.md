# Apache Spark Assessment 1
This Blueprint assessment is used to evaluate your comfort with Apache Spark. To submit your answer(s), create a pull request to this repo with changes to this file that include the answers to the question(s) below.

For questions about this assessment, submit an [Issue](https://github.com/BlueprintTechnologies/blueprint-tech-assessments/issues). 


## Essay Question
In 200 words **or more**, describe the architecture of Apache Spark.

The architecture of Apache Spark is a master-slave architecture.
The entrypoint of the application is a main driver program, known as a SparkContext, which implements the logic of the application and communicates with the cluster manager to distribute jobs across worker nodes.
The application code gets converted by Spark into a DAG (Directed Acyclic Graph), which encodes the operations to be performed on the data.
This DAG gets passed to the cluster manager, which handles the distribution of the workload across the cluster.
Within each worker node is an Executor which handles the computation of task assigned by the cluster manager to a worker node.
The results of these tasks is stored in a Cache, which will be retrieved according to the logic of the invoking application.
The data is interfaced to the driver through an RDD (Resilient Distributed Dataset) interface, which abstracts the underlying computation.
The RDD interface abstracts recomputation in case of failures (resilient) and partition of data across worker nodes (Distributed).
This provides a convenience to the end user writing the application code.
Data on worker nodes is stored on top of Apache Hadoop Distributed File System (HDFS).
Apache Spark is implemented in Java and applications are JVM (Java Virtual Machine) processes.


## Coding Example
As part of your pull request to this repo, include a jupyter notebook that includes your custom code that demonstrates Apache Spark usage across all six parts below.

Part 1: Using Apache Spark, demonstrate how you read data from https://ride.capitalbikeshare.com/system-data.

Part 2: Using Apache Spark, demonstrate what would you do to the data so that, in the future, you can run efficient analytics on it?

Part 3: Using Apache Spark, demonstrate how you delete the latitude related columns.

Part 4: Using Apache Spark, demonstrate how you add duration of each ride.

Part 5: Using Apache Spark, demonstrate how you calculate average ride duration for each rideable type.

Part 6: Using Apache Spark, demonstrate how you calculate top 10 ride durations (longer than 24 hours) for each start station.
