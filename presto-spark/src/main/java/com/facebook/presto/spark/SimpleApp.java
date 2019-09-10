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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeUtil;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.index.IndexHandleJacksonModule;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
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
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spark.SparkOutputBuffer.PageWithPartition;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionDeserializer;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionSerializer;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.stream.Collectors.toList;

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
        String sql = "select * from tpch.tiny.lineitem where orderkey = 14983 OR orderkey = 15009";
        Plan plan = localQueryRunner.createPlan(sql, WarningCollector.NOOP);
        SubPlan subPlan = localQueryRunner.createSubPlan(plan);
        checkState(subPlan.getChildren().isEmpty());

        PlanFragment planFragment = subPlan.getFragment();
        List<TaskSource> taskSources = localQueryRunner.getTaskSources(subPlan.getFragment());

        List<SparkTaskRequest> taskRequests = taskSources.stream()
                // TODO: Now it's a simple "one split per Spark worker" model
                .flatMap(taskSource -> taskSource.getSplits().stream()
                        .map(split -> new SparkTaskRequest(
                                planFragment,
                                ImmutableList.of(
                                        new TaskSource(
                                                taskSource.getPlanNodeId(),
                                                ImmutableSet.of(split),
                                                taskSource.getNoMoreSplitsForLifespan(),
                                                taskSource.isNoMoreSplits())))))
                // .map(taskSource -> new SparkTaskRequest(planFragment, ImmutableList.of(taskSource)))
                .collect(toImmutableList());

        // Similar to  https://github.com/prestodb/presto/blob/67ee4c09a2549c22fd208ab8140ef1a86a9c953f/presto-main/src/main/java/com/facebook/presto/server/remotetask/HttpRemoteTask.java#L621
        JsonCodec<SparkTaskRequest> jsonCodec = createJsonCodec(SparkTaskRequest.class, localQueryRunner.getHandleResolver());
        List<String> serializedRequests = taskRequests.stream()
                .map(jsonCodec::toJson)
                .collect(toImmutableList());

        List<byte[]> serializedResult = jsc.parallelize(serializedRequests)
                .flatMapToPair(request -> handleSparkWorkerRequest(request))
                .map(tupple -> tupple._2)
                .collect();

        PagesSerde pagesSerde = createPagesSerde();

        List<Page> result = serializedResult.stream()
                .map(bytes -> deserializePage(pagesSerde, bytes))
                .collect(toList());

        System.out.println("Results: " + result.size());

        result.stream()
                .map(page -> getPageValues(page, planFragment.getTypes()))
                .flatMap(List::stream)
                .forEach(System.out::println);

        jsc.stop();
    }

    private static Iterator<Tuple2<Integer, byte[]>> handleSparkWorkerRequest(String serializedRequest)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner(createSession());
        SparkTaskRequest request = createJsonCodec(SparkTaskRequest.class, localQueryRunner.getHandleResolver()).fromJson(serializedRequest);
        System.out.println(request.getFragment());
        System.out.println(request.getSources());

        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(2, GIGABYTE));
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(
                new QueryId("test"),
                new DataSize(1, GIGABYTE),
                new DataSize(2, GIGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                localQueryRunner.getExecutor(),
                localQueryRunner.getScheduler(),
                new DataSize(4, GIGABYTE),
                spillSpaceTracker);

        TaskContext taskContext = queryContext
                .addTaskContext(new TaskStateMachine(new TaskId("query", 0, 0), localQueryRunner.getExecutor()),
                        localQueryRunner.getDefaultSession(),
                        false,
                        false,
                        OptionalInt.empty(),
                        false);

        SparkOutputBuffer outputBuffer = new SparkOutputBuffer();
        LocalExecutionPlan localExecutionPlan = localQueryRunner.createLocalExecutionPlan(outputBuffer, taskContext, request.getFragment());
        List<Driver> drivers = localQueryRunner.createDrivers(
                localExecutionPlan,
                taskContext,
                request.getFragment(),
                request.getSources());

        return new SparkDriverProcessor(drivers, outputBuffer, createPagesSerde());
    }

    private static class SparkDriverProcessor
            extends AbstractIterator<Tuple2<Integer, byte[]>>
    {
        private final List<Driver> drivers;
        private final SparkOutputBuffer outputBuffer;
        private final PagesSerde serde;

        private SparkDriverProcessor(List<Driver> drivers, SparkOutputBuffer outputBuffer, PagesSerde serde)
        {
            this.drivers = drivers;
            this.outputBuffer = outputBuffer;
            this.serde = serde;
        }

        @Override
        protected Tuple2<Integer, byte[]> computeNext()
        {
            boolean done = false;
            while (!done && !outputBuffer.hasPagesBuffered()) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (!driver.isFinished()) {
                        driver.process();
                        processed = true;
                    }
                }
                done = !processed;
            }

            if (done && !outputBuffer.hasPagesBuffered()) {
                return endOfData();
            }

            PageWithPartition pageWithPartition = outputBuffer.getNext();
            SerializedPage serializedPage = pageWithPartition.getPage();
            return new Tuple2<>(pageWithPartition.getPartition(), serializePage(serializedPage));
        }
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
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(4), ImmutableMap.of());
        return localQueryRunner;
    }

    private static <T> JsonCodec<T> createJsonCodec(Class<T> clazz, HandleResolver handleResolver)
    {
        TypeManager typeManager = new TypeRegistry();
        BlockEncodingSerde serde = new BlockEncodingManager(typeManager);

        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setKeySerializers(ImmutableMap.of(
                VariableReferenceExpression.class, new VariableReferenceExpressionSerializer()));
        provider.setKeyDeserializers(ImmutableMap.of(
                VariableReferenceExpression.class, new VariableReferenceExpressionDeserializer(typeManager)));
        provider.setJsonSerializers(ImmutableMap.of(
                Block.class, new BlockJsonSerde.Serializer(serde)));
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(typeManager),
                Block.class, new BlockJsonSerde.Deserializer(serde)));
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

    private static PagesSerde createPagesSerde()
    {
        TypeManager typeManager = new TypeRegistry();
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(typeManager, ImmutableSet.of());
        return new PagesSerde(blockEncodingManager, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static byte[] serializePage(PagesSerde pagesSerde, Page page)
    {
        return serializePage(pagesSerde.serialize(page));
    }

    private static byte[] serializePage(SerializedPage page)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput(page.getUncompressedSizeInBytes());
        PagesSerdeUtil.writeSerializedPage(sliceOutput, page);
        try {
            sliceOutput.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return sliceOutput.getUnderlyingSlice().getBytes();
    }

    private static Page deserializePage(PagesSerde pagesSerde, byte[] data)
    {
        SliceInput sliceInput = Slices.wrappedBuffer(data).getInput();
        SerializedPage serializedPage = PagesSerdeUtil.readSerializedPages(sliceInput).next();
        return pagesSerde.deserialize(serializedPage);
    }

    private static List<List<Object>> getPageValues(Page page, List<Type> types)
    {
        ConnectorSession connectorSession = createSession().toConnectorSession();
        List<List<Object>> result = new ArrayList<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> values = new ArrayList<>(page.getChannelCount());
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Type type = types.get(channel);
                Block block = page.getBlock(channel);

                values.add(type.getObjectValue(connectorSession, block, position));
            }
            result.add(Collections.unmodifiableList(values));
        }
        return Collections.unmodifiableList(result);
    }
}
