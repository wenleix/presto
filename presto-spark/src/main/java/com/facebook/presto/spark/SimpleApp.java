/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class SimpleApp
{
    private SimpleApp() {}

    private static final JsonCodec<SparkTaskRequest> SPARK_TASK_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(SparkTaskRequest.class);

    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Simple Query");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        // === Create LocalQueryRunner ===

        // TODO: QueryRunner is originally designed for test purpose. Rethink about the abstraction
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = LocalQueryRunner.queryRunnerWithInitialTransaction(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        // do some interesting work...
        String sql = "select * from tpch.sf1.lineitem";
        Plan plan = localQueryRunner.createPlan(sql, WarningCollector.NOOP);
        SubPlan subPlan = localQueryRunner.createSubPlan(plan);
        checkState(subPlan.getChildren().isEmpty());

        PlanFragment planFragment = subPlan.getFragment();
        List<TaskSource> taskSources = localQueryRunner.getTaskSources(subPlan.getFragment());

        List<SparkTaskRequest> taskRequests = taskSources.stream()
                // TODO: Now it's a simple "one split per Spark worker" model
                .map(taskSource -> new SparkTaskRequest(planFragment, ImmutableList.of(taskSource)))
                .collect(toImmutableList());

        // Similar to  https://github.com/prestodb/presto/blob/67ee4c09a2549c22fd208ab8140ef1a86a9c953f/presto-main/src/main/java/com/facebook/presto/server/remotetask/HttpRemoteTask.java#L621
        List<String> serializedRequests = taskRequests.stream()
                .map(SPARK_TASK_REQUEST_JSON_CODEC::toJson)
                .collect(toImmutableList());

        SparkTaskRequest request = SPARK_TASK_REQUEST_JSON_CODEC.fromJson(serializedRequests.get(0));

        jsc.parallelize(serializedRequests)
                .foreach(serializedRequest -> {
                    System.out.println(serializedRequest);
                    SparkTaskRequest request2 = JsonCodec.jsonCodec(SparkTaskRequest.class).fromJson(serializedRequest);
                    System.out.println(request.getFragment());
                    System.out.println(request.getSources());
                });

        jsc.stop();
    }
}
