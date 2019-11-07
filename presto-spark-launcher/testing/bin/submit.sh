#!/bin/bash

set -ex

VERSION="0.230-SNAPSHOT"

SPARK_APPLICATION_JAR_LOCATION="/presto/presto-spark-launcher/target/presto-spark-launcher-${VERSION}-shaded.jar"
SPARK_APPLICATION_MAIN_CLASS="com.facebook.presto.spark.launcher.PrestoSparkLauncher"

QUERY_FILE_LOCATION="/presto/presto-spark-launcher/testing/queries/aggregation.sql"
PACKAGE_LOCATION="/presto/presto-spark-package/target/presto-spark-package-${VERSION}.tar.gz"
CONFIG_LOCATION="/presto/presto-spark-launcher/testing/etc/config.properties"
CATALOGS_LOCATION="/presto/presto-spark-launcher/testing/etc/catalogs"

SPARK_SUBMIT_ARGS="--deploy-mode client"
SPARK_APPLICATION_ARGS="-f ${QUERY_FILE_LOCATION} --package ${PACKAGE_LOCATION} --config ${CONFIG_LOCATION} --catalogs ${CATALOGS_LOCATION}"

docker run \
  --rm \
  -v /home/andriirosa/projects/presto:/presto \
  --network docker_spark_cluster_network \
  --env SPARK_APPLICATION_JAR_LOCATION="$SPARK_APPLICATION_JAR_LOCATION" \
  --env SPARK_APPLICATION_MAIN_CLASS="$SPARK_APPLICATION_MAIN_CLASS" \
  --env SPARK_SUBMIT_ARGS="$SPARK_SUBMIT_ARGS" \
  --env SPARK_APPLICATION_ARGS="$SPARK_APPLICATION_ARGS" \
  spydernaz/spark-submit:2.4.3