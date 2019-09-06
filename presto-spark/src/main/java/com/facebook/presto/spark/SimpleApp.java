package com.facebook.presto.spark;

import com.facebook.presto.hadoop.$internal.com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {
    public static void main(String[] args) {

        /*
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Simple Query");

        JavaSparkContext sc = new JavaSparkContext(conf);
        */

        // SparkSession is preferred, see SPARK-15031
        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Query")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Cannot use System.err::println since Spark will try to serialize System.err
        jsc.parallelize(ImmutableList.of(1, 2, 3, 4, 5, 6, 7))
                .foreach(x -> System.err.println(x));

        // TODO: QueryRunner is originally designed for test purpose. Rethink about the abstraction
//        POSLocalQueryRunner queryRunner = POSQueryRunner.createLocalQueryRunner();
    }
}
