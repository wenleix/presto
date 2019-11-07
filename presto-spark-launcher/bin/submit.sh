#!/bin/bash

SPARK_APPLICATION_JAR_LOCATION="/presto/presto-spark-launcher/target/presto-spark-launcher-0.230-SNAPSHOT-shaded.jar"
SPARK_APPLICATION_MAIN_CLASS="com.facebook.presto.spark.launcher.PrestoSparkLauncher"

QUERY_FILE_LOCATION="/presto/presto-spark-launcher/queries/aggregation.sql"
PACKAGE_LOCATION="/presto/presto-spark-package/target/presto-spark-package-0.230-SNAPSHOT.tar.gz"
CONFIG_LOCATION="/presto/presto-spark-launcher/etc/config.properties"
CATALOGS_LOCATION="/presto/presto-spark-launcher/etc/catalogs"

SPARK_SUBMIT_ARGS=""
SPARK_APPLICATION_ARGS="-f ${QUERY_FILE_LOCATION} --package ${PACKAGE_LOCATION} --config ${CONFIG_LOCATION} --catalogs ${CATALOGS_LOCATION}"

docker run \
  --network docker-spark-cluster_default \
  -v /home/andriirosa/projects/presto:/presto \
  --env SPARK_APPLICATION_JAR_LOCATION="$SPARK_APPLICATION_JAR_LOCATION" \
  --env SPARK_APPLICATION_MAIN_CLASS="$SPARK_APPLICATION_MAIN_CLASS" \
  --env SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS" \
  --env SPARK_APPLICATION_ARGS="$SPARK_APPLICATION_ARGS" \
  spark-submit:latest