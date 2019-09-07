package com.facebook.presto.spark;

import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.hadoop.$internal.com.google.common.collect.ImmutableList;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import io.airlift.json.JsonCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class SimpleApp {
    private static final JsonCodec<TaskSource> TASK_SOURCE_JSON_CODEC = JsonCodec.jsonCodec(TaskSource.class);

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
//        jsc.parallelize(ImmutableList.of(1, 2, 3, 4, 5, 6, 7))
//                .foreach(x -> System.err.println(x));

        // TODO: QueryRunner is originally designed for test purpose. Rethink about the abstraction
        POSLocalQueryRunner queryRunner = POSQueryRunner.createLocalQueryRunner();
        String sql = "select * from tpch.sf1.lineitem";

        LocalExecutionPlanner.LocalExecutionPlan plan = queryRunner.getLocalExecutionPlan(sql);

        // Useful information in TaskSource for Presto-on-Spark is planNodeId and split
        // In real case, it should be List<(planNodeId + Split)> + PlanFragment sending to each Spark worker
        // Splits has to be bundled for fragment with multiple sources (e.g. JOIN)
        List<TaskSource> taskSources = queryRunner.getTaskSources(sql);

        // Similar to  https://github.com/prestodb/presto/blob/67ee4c09a2549c22fd208ab8140ef1a86a9c953f/presto-main/src/main/java/com/facebook/presto/server/remotetask/HttpRemoteTask.java#L621
        List<String> serializedTaskSources = taskSources.stream()
                .map(taskSource -> TASK_SOURCE_JSON_CODEC.toJson(taskSource))
                .collect(toImmutableList());

        jsc.parallelize(serializedTaskSources)
                .foreach(serializedTaskSource -> System.out.println(serializedTaskSources));

        spark.stop();
    }
}
