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
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionDeserializer;
import com.facebook.presto.sql.Serialization.VariableReferenceExpressionSerializer;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SparkQueryRunner
{
    private final PrestoConfiguration prestoConfiguration;
    private final SparkConf sparkConfiguration;
    private final int partitions;

    public SparkQueryRunner(PrestoConfiguration prestoConfiguration, SparkConf sparkConfiguration, int partitions)
    {
        this.prestoConfiguration = requireNonNull(prestoConfiguration, "prestoConfiguration is null");
        this.sparkConfiguration = requireNonNull(sparkConfiguration, "sparkConfiguration is null");
        this.partitions = partitions;
    }

    public static void main(String[] args)
    {
        int workers = 3;
        int partitions = 6;

        SparkConf sparkConfiguration = new SparkConf()
                .setMaster(format("local[%s]", workers))
                .setAppName("Simple Query");

        PrestoConfiguration prestoConfiguration = new PrestoConfiguration(
                ImmutableList.of("com.facebook.presto.tpch.TpchPlugin"),
                ImmutableList.of(
                        new CatalogConfiguration("tpch", "tpch", ImmutableMap.of())));

        new SparkQueryRunner(prestoConfiguration, sparkConfiguration, partitions)
                .run("select partkey, count(*) c from tpch.tiny.lineitem where partkey % 10 = 1 group by partkey having count(*) = 42");
    }

    public void run(String query)
    {
        JavaSparkContext jsc = new JavaSparkContext(sparkConfiguration);

        // === Create LocalQueryRunner ===
        LocalQueryRunner localQueryRunner = createLocalQueryRunner(prestoConfiguration);

        // do some interesting work...
        Plan plan = localQueryRunner.createPlan(localQueryRunner.getDefaultSession(), query, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP);
        SubPlan subPlan = localQueryRunner.createDistributedSubPlan(plan);

        List<byte[]> serializedResult = createSparkRdd(localQueryRunner, jsc, subPlan, prestoConfiguration, partitions)
                .map(tupple -> tupple._2)
                .collect();

        PagesSerde pagesSerde = createPagesSerde();

        List<Page> result = serializedResult.stream()
                .map(bytes -> deserializePage(pagesSerde, bytes))
                .collect(toList());

        System.out.println("Results: " + result.size());

        result.stream()
                .map(page -> getPageValues(page, subPlan.getFragment().getTypes()))
                .flatMap(List::stream)
                .forEach(System.out::println);

        jsc.stop();
    }

    private static JavaPairRDD<Integer, byte[]> createSparkRdd(LocalQueryRunner localQueryRunner, JavaSparkContext jsc, SubPlan subPlan, PrestoConfiguration prestoConfiguration, int partitions)
    {
        PlanFragment fragment;
        if (subPlan.getFragment().getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION)) {
            fragment = subPlan.getFragment().withBucketToPartition(Optional.of(IntStream.range(0, partitions).toArray()));
        }
        else {
            fragment = subPlan.getFragment();
        }

        checkArgument(!fragment.getStageExecutionDescriptor().isStageGroupedExecution(), "unexpected grouped execution fragment: %s", fragment.getId());

        // scans
        List<PlanNodeId> tableScans = fragment.getTableScanSchedulingOrder();

        // source stages
        List<RemoteSourceNode> remoteSources = fragment.getRemoteSourceNodes();
        checkArgument(tableScans.isEmpty() || remoteSources.isEmpty(), "stage has both, remote sources and table scans");

        JsonCodec<SparkTaskRequest> jsonCodec = createJsonCodec(SparkTaskRequest.class, localQueryRunner.getHandleResolver());

        if (!tableScans.isEmpty()) {
            checkArgument(fragment.getPartitioning().equals(SOURCE_DISTRIBUTION), "unexpected table scan partitioning: %s", fragment.getPartitioning());

            List<TaskSource> taskSources = localQueryRunner.getTaskSources(subPlan.getFragment());
            List<SparkTaskRequest> taskRequests = taskSources.stream()
                    .flatMap(taskSource -> taskSource.getSplits().stream()
                            .map(split -> new SparkTaskRequest(
                                    fragment,
                                    ImmutableList.of(
                                            new TaskSource(
                                                    taskSource.getPlanNodeId(),
                                                    ImmutableSet.of(split),
                                                    taskSource.getNoMoreSplitsForLifespan(),
                                                    taskSource.isNoMoreSplits())))))
                    .collect(toImmutableList());
            List<String> serializedRequests = taskRequests.stream()
                    .map(jsonCodec::toJson)
                    .collect(toImmutableList());
            return jsc.parallelize(serializedRequests)
                    .flatMapToPair(request -> handleSparkWorkerRequest(request, ImmutableMap.of(), prestoConfiguration));
        }

        List<SubPlan> children = subPlan.getChildren();
        checkArgument(
                remoteSources.size() == children.size(),
                "number of remote sources doesn't match the number of child stages: %s != %s",
                remoteSources.size(),
                children.size());
        checkArgument(children.size() == 1, "expected to have exactly single children");
        SubPlan childSubPlan = getOnlyElement(children);
        JavaPairRDD<Integer, byte[]> childRdd = createSparkRdd(localQueryRunner, jsc, childSubPlan, prestoConfiguration, partitions);

        PlanFragment childFragment = childSubPlan.getFragment();
        RemoteSourceNode remoteSource = getOnlyElement(remoteSources);
        List<PlanFragmentId> sourceFragmentIds = remoteSource.getSourceFragmentIds();
        checkArgument(sourceFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
        checkArgument(childFragment.getId().equals(getOnlyElement(sourceFragmentIds)));

        SparkTaskRequest sparkTaskRequest = new SparkTaskRequest(fragment, ImmutableList.of());
        String serializedRequest = jsonCodec.toJson(sparkTaskRequest);

        PartitioningHandle partitioning = fragment.getPartitioning();
        if (partitioning.equals(COORDINATOR_DISTRIBUTION)) {
            // TODO: We assume COORDINATOR_DISTRIBUTION always means OutputNode
            // But it could also be TableFinishNode, in that case we should do collect and table commit on coordinator.

            // TODO: Do we want to return an RDD for root stage? -- or we should consider the result will not be large?
            List<Tuple2<Integer, byte[]>> collect = childRdd.collect();
            List<Tuple2<Integer, byte[]>> result = ImmutableList.copyOf(handleSparkWorkerRequest(serializedRequest, ImmutableMap.of(remoteSource.getId(), collect.iterator()), prestoConfiguration));
            return jsc.parallelizePairs(result);
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) ||
                // when single distribution - there will be a single partition 0
                partitioning.equals(SINGLE_DISTRIBUTION)) {
            String planNodeId = remoteSource.getId().toString();
            return childRdd
                    .partitionBy(partitioning.equals(FIXED_HASH_DISTRIBUTION) ? new HashPartitioner(partitions) : new HashPartitioner(1))
                    .mapPartitionsToPair(iterator -> handleSparkWorkerRequest(serializedRequest, ImmutableMap.of(new PlanNodeId(planNodeId), iterator), prestoConfiguration));
        }
        else {
            // SOURCE_DISTRIBUTION || FIXED_PASSTHROUGH_DISTRIBUTION || ARBITRARY_DISTRIBUTION || SCALED_WRITER_DISTRIBUTION || FIXED_BROADCAST_DISTRIBUTION || FIXED_ARBITRARY_DISTRIBUTION
            throw new IllegalArgumentException("Unsupported fragment partitioning: " + partitioning);
        }
    }

    private static Iterator<Tuple2<Integer, byte[]>> handleSparkWorkerRequest(String serializedRequest, Map<PlanNodeId, Iterator<Tuple2<Integer, byte[]>>> inputs, PrestoConfiguration prestoConfiguration)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner(prestoConfiguration);
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
        LocalExecutionPlan localExecutionPlan = localQueryRunner.createLocalExecutionPlan(
                outputBuffer,
                taskContext,
                request.getFragment(),
                inputs.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> Iterators.transform(entry.getValue(), tupple -> deserializeSerializedPage(tupple._2)))));
        List<Driver> drivers = localQueryRunner.createDrivers(
                localExecutionPlan,
                taskContext,
                request.getFragment(),
                request.getSources());

        String description = format("fragment: %s, processor: %s", request.getFragment().getId(), UUID.randomUUID().toString());
        return new SparkDriverProcessor(description, drivers, outputBuffer, createPagesSerde());
    }

    private static class SparkDriverProcessor
            extends AbstractIterator<Tuple2<Integer, byte[]>>
    {
        private final String description;
        private final List<Driver> drivers;
        private final SparkOutputBuffer outputBuffer;
        private final PagesSerde serde;

        private SparkDriverProcessor(String description, List<Driver> drivers, SparkOutputBuffer outputBuffer, PagesSerde serde)
        {
            this.description = description;
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
            System.out.printf("Producing page for partition: %s (%s)\n", pageWithPartition.getPartition(), description);
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

    private static LocalQueryRunner createLocalQueryRunner(PrestoConfiguration prestoConfiguration)
    {
        LocalQueryRunner localQueryRunner = LocalQueryRunner.queryRunnerWithInitialTransaction(createSession());

        for (String plugin : prestoConfiguration.getPlugins()) {
            try {
                localQueryRunner.installPlugin((Plugin) Class.forName(plugin).newInstance());
            }
            catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        for (CatalogConfiguration catalog : prestoConfiguration.getCatalogs()) {
            localQueryRunner.createCatalog(catalog.getCatalogName(), catalog.getPluginName(), catalog.getCatalogConfiguration());
        }

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
        return pagesSerde.deserialize(deserializeSerializedPage(data));
    }

    private static SerializedPage deserializeSerializedPage(byte[] data)
    {
        return PagesSerdeUtil.readSerializedPages(Slices.wrappedBuffer(data).getInput()).next();
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

    public static class PrestoConfiguration
            implements Serializable
    {
        private final List<String> plugins;
        private final List<CatalogConfiguration> catalogs;

        public PrestoConfiguration(List<String> plugins, List<CatalogConfiguration> catalogs)
        {
            this.plugins = ImmutableList.copyOf(plugins);
            this.catalogs = ImmutableList.copyOf(catalogs);
        }

        public List<String> getPlugins()
        {
            return plugins;
        }

        public List<CatalogConfiguration> getCatalogs()
        {
            return catalogs;
        }
    }

    public static class CatalogConfiguration
            implements Serializable
    {
        private final String pluginName;
        private final String catalogName;
        private final Map<String, String> catalogConfiguration;

        public CatalogConfiguration(String pluginName, String catalogName, Map<String, String> catalogConfiguration)
        {
            this.pluginName = pluginName;
            this.catalogName = catalogName;
            this.catalogConfiguration = ImmutableMap.copyOf(catalogConfiguration);
        }

        public String getPluginName()
        {
            return pluginName;
        }

        public String getCatalogName()
        {
            return catalogName;
        }

        public Map<String, String> getCatalogConfiguration()
        {
            return catalogConfiguration;
        }
    }
}
