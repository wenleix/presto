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
import com.facebook.presto.index.IndexHandleJacksonModule;
import com.facebook.presto.metadata.ColumnHandleJacksonModule;
import com.facebook.presto.metadata.FunctionHandleJacksonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InsertTableHandleJacksonModule;
import com.facebook.presto.metadata.OutputTableHandleJacksonModule;
import com.facebook.presto.metadata.PartitioningHandleJacksonModule;
import com.facebook.presto.metadata.SplitJacksonModule;
import com.facebook.presto.metadata.TableHandleJacksonModule;
import com.facebook.presto.metadata.TableLayoutHandleJacksonModule;
import com.facebook.presto.metadata.TransactionHandleJacksonModule;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionDeserializer;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionSerializer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
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

    public static void main(String[] args)
    {
        SparkConf conf = new SparkConf()
                .setMaster("local[4]")
                .setAppName("Simple Query");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        // === Create LocalQueryRunner ===

        // TODO: QueryRunner is originally designed for test purpose. Rethink about the abstraction
        Session session = createSession();

        LocalQueryRunner localQueryRunner = createLocalQueryRunner(session);

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
        JsonCodec<SparkTaskRequest> jsonCodec = createJsonCodec(SparkTaskRequest.class, localQueryRunner.getHandleResolver());
        List<String> serializedRequests = taskRequests.stream()
                .map(jsonCodec::toJson)
                .collect(toImmutableList());

        jsc.parallelize(serializedRequests)
                .foreach(serializedRequest -> {
                    handleSparkWorkerRequest(serializedRequest);
                });

        jsc.stop();
    }

    private static void handleSparkWorkerRequest(String serializedRequest)
    {
        LocalQueryRunner localQueryRunnerRemote = createLocalQueryRunner(createSession());
        System.out.println(serializedRequest);
        SparkTaskRequest request2 = createJsonCodec(SparkTaskRequest.class, localQueryRunnerRemote.getHandleResolver()).fromJson(serializedRequest);
        System.out.println(request2.getFragment());
        System.out.println(request2.getSources());
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();
    }

    private static LocalQueryRunner createLocalQueryRunner(Session session)
    {
        LocalQueryRunner localQueryRunner = LocalQueryRunner.queryRunnerWithInitialTransaction(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return localQueryRunner;
    }

    private static <T> JsonCodec<T> createJsonCodec(Class<T> clazz, HandleResolver handleResolver)
    {
        TypeManager typeManager = new TypeRegistry();
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setKeySerializers(ImmutableMap.of(
                VariableReferenceExpression.class, new VariableReferenceExpressionSerializer()
        ));
        provider.setKeyDeserializers(ImmutableMap.of(
                VariableReferenceExpression.class, new VariableReferenceExpressionDeserializer(typeManager)
        ));
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(typeManager)
        ));
        provider.setModules(ImmutableSet.of(
                new TableHandleJacksonModule(handleResolver),
                new TableLayoutHandleJacksonModule(handleResolver),
                new ColumnHandleJacksonModule(handleResolver),
                new SplitJacksonModule(handleResolver),
                new OutputTableHandleJacksonModule(handleResolver),
                new InsertTableHandleJacksonModule(handleResolver),
                new IndexHandleJacksonModule(handleResolver),
                new TransactionHandleJacksonModule(handleResolver),
                new PartitioningHandleJacksonModule(handleResolver),
                new FunctionHandleJacksonModule(handleResolver)));
        JsonCodecFactory factory = new JsonCodecFactory(provider);
        return factory.jsonCodec(clazz);
    }
}
