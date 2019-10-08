#!/bin/bash

SPARK_APPLICATION_JAR_LOCATION="/presto/presto-spark-runner/target/presto-spark-runner-0.228-SNAPSHOT-shaded.jar"
SPARK_APPLICATION_MAIN_CLASS="com.facebook.presto.spark.runner.PrestoSparkRunner"
#SPARK_SUBMIT_ARGS="--jars /repository/com/fasterxml/jackson/core/jackson-core/2.9.7/jackson-core-2.9.7.jar,/repository/com/fasterxml/jackson/core/jackson-databind/2.9.7/jackson-databind-2.9.7.jar,/repository/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.9.7/jackson-datatype-jdk8-2.9.7.jar,/repository/com/google/code/findbugs/jsr305/3.0.2/jsr305-3.0.2.jar,/repository/com/google/errorprone/error_prone_annotations/2.1.3/error_prone_annotations-2.1.3.jar,/repository/com/google/guava/guava/24.1-jre/guava-24.1-jre.jar,/repository/com/google/j2objc/j2objc-annotations/1.1/j2objc-annotations-1.1.jar,/repository/io/airlift/tpch/tpch/0.9/tpch-0.9.jar,/repository/org/checkerframework/checker-compat-qual/2.0.0/checker-compat-qual-2.0.0.jar,/repository/org/codehaus/mojo/animal-sniffer-annotations/1.14/animal-sniffer-annotations-1.14.jar,/presto/presto-tpch/target/presto-tpch-0.228-SNAPSHOT.jar"
SPARK_SUBMIT_ARGS=""
SPARK_APPLICATION_ARGS="/presto/presto-spark/target/presto-spark-0.228-SNAPSHOT-shaded.jar"

docker run \
  --network docker-spark-cluster_default \
  -v /home/andriirosa/projects/presto:/presto \
  -v /home/andriirosa/.m2/repository:/repository \
  --env SPARK_APPLICATION_JAR_LOCATION="$SPARK_APPLICATION_JAR_LOCATION" \
  --env SPARK_APPLICATION_MAIN_CLASS="$SPARK_APPLICATION_MAIN_CLASS" \
  --env SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS" \
  --env SPARK_APPLICATION_ARGS="$SPARK_APPLICATION_ARGS" \
  spark-submit:latest