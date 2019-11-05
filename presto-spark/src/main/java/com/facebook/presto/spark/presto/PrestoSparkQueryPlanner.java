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
package com.facebook.presto.spark.presto;

import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.spark.spi.PrestoConfiguration;
import com.facebook.presto.spark.spi.SparkFragmentCompiler;
import com.facebook.presto.spark.spi.SparkPlan;
import com.facebook.presto.spark.spi.SparkQueryPlanner;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.spark.presto.LocalQueryRunnerFactory.createJsonCodec;
import static com.facebook.presto.spark.presto.LocalQueryRunnerFactory.createLocalQueryRunner;
import static com.facebook.presto.spark.spi.SerializableProcessors.createSerializableProcessor;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class PrestoSparkQueryPlanner
        implements SparkQueryPlanner
{
    private final SparkContext sparkContext;
    private final PrestoConfiguration prestoConfiguration;
    private final int partitions;

    public PrestoSparkQueryPlanner(SparkContext sparkContext, PrestoConfiguration prestoConfiguration, int partitions)
    {
        this.sparkContext = requireNonNull(sparkContext, "sparkContext is null");
        this.prestoConfiguration = requireNonNull(prestoConfiguration, "prestoConfiguration is null");
        this.partitions = partitions;
    }

    @Override
    public SparkPlan plan(String query, Supplier<SparkFragmentCompiler> compiler)
    {
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);

        // === Create LocalQueryRunner ===
        LocalQueryRunner localQueryRunner = createLocalQueryRunner(prestoConfiguration);

        // do some interesting work...
        Plan plan = localQueryRunner.createPlan(localQueryRunner.getDefaultSession(), query, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP);
        SubPlan subPlan = localQueryRunner.createDistributedSubPlan(plan);

        return new SparkPlan(createSparkRdd(localQueryRunner, jsc, subPlan, partitions, compiler), ImmutableList.copyOf(subPlan.getFragment().getTypes()));
    }

    private static JavaPairRDD<Integer, byte[]> createSparkRdd(
            LocalQueryRunner localQueryRunner,
            JavaSparkContext jsc,
            SubPlan subPlan,
            int partitions,
            Supplier<SparkFragmentCompiler> compiler)
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
                    .flatMapToPair(createSerializableProcessor(compiler));
        }

        List<SubPlan> children = subPlan.getChildren();
        checkArgument(
                remoteSources.size() == children.size(),
                "number of remote sources doesn't match the number of child stages: %s != %s",
                remoteSources.size(),
                children.size());

        if (children.size() == 1) {
            // Single remote source
            SubPlan childSubPlan = getOnlyElement(children);
            JavaPairRDD<Integer, byte[]> childRdd = createSparkRdd(localQueryRunner, jsc, childSubPlan, partitions, compiler);

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
                List<Tuple2<Integer, byte[]>> result = ImmutableList.copyOf(
                        compiler.get().compileFragment(
                                serializedRequest,
                                ImmutableMap.of(
                                        remoteSource.getId().toString(),
                                        collect.iterator())));
                return jsc.<Integer, byte[]>parallelizePairs(result);
            }
            else if (partitioning.equals(FIXED_HASH_DISTRIBUTION) ||
                    // when single distribution - there will be a single partition 0
                    partitioning.equals(SINGLE_DISTRIBUTION)) {
                String planNodeId = remoteSource.getId().toString();
                return childRdd
                        // TODO: What's the difference of using
                        //  partitionBy/mapPartitionsToPair vs. groupBy vs. mapToPair ???
                        .partitionBy(partitioning.equals(FIXED_HASH_DISTRIBUTION) ? new HashPartitioner(partitions) : new HashPartitioner(1))
                        .mapPartitionsToPair(createSerializableProcessor(compiler, serializedRequest, planNodeId));
            }
            else {
                // SOURCE_DISTRIBUTION || FIXED_PASSTHROUGH_DISTRIBUTION || ARBITRARY_DISTRIBUTION || SCALED_WRITER_DISTRIBUTION || FIXED_BROADCAST_DISTRIBUTION || FIXED_ARBITRARY_DISTRIBUTION
                throw new IllegalArgumentException("Unsupported fragment partitioning: " + partitioning);
            }
        }
        else if (children.size() == 2) {
            // handle simple two-way join
            SubPlan leftSubPlan = children.get(0);
            SubPlan rightSubPlan = children.get(1);

            RemoteSourceNode leftRemoteSource = remoteSources.get(0);
            RemoteSourceNode rightRemoteSource = remoteSources.get(1);

            // We need String representation since PlanNodeId is not serializable...
            String leftRemoteSourcePlanId = leftRemoteSource.getId().toString();
            String rightRemoteSourcePlanId = rightRemoteSource.getId().toString();

            JavaPairRDD<Integer, byte[]> leftChildRdd = createSparkRdd(localQueryRunner, jsc, leftSubPlan, partitions, compiler);
            JavaPairRDD<Integer, byte[]> rightChildRdd = createSparkRdd(localQueryRunner, jsc, rightSubPlan, partitions, compiler);

            PlanFragment leftFragment = leftSubPlan.getFragment();
            PlanFragment rightFragment = rightSubPlan.getFragment();

            List<PlanFragmentId> leftFragmentIds = leftRemoteSource.getSourceFragmentIds();
            checkArgument(leftFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
            checkArgument(leftFragment.getId().equals(getOnlyElement(leftFragmentIds)));
            List<PlanFragmentId> rightFragmentIds = rightRemoteSource.getSourceFragmentIds();
            checkArgument(rightFragmentIds.size() == 1, "expected to have exactly only a single source fragment");
            checkArgument(rightFragment.getId().equals(getOnlyElement(rightFragmentIds)));

            // This fragment only contains remote source, thus there is no splits
            SparkTaskRequest sparkTaskRequest = new SparkTaskRequest(fragment, ImmutableList.of());
            String serializedRequest = jsonCodec.toJson(sparkTaskRequest);

            PartitioningHandle partitioning = fragment.getPartitioning();
            checkArgument(partitioning.equals(FIXED_HASH_DISTRIBUTION));

            JavaPairRDD<Integer, byte[]> shuffledLeftChildRdd = leftChildRdd.partitionBy(new HashPartitioner(partitions));
            JavaPairRDD<Integer, byte[]> shuffledRightChildRdd = rightChildRdd.partitionBy(new HashPartitioner(partitions));

            return JavaPairRDD.fromJavaRDD(
                    shuffledLeftChildRdd.zipPartitions(
                            shuffledRightChildRdd,
                            createSerializableProcessor(compiler, serializedRequest, leftRemoteSourcePlanId, rightRemoteSourcePlanId)));
        }
        else {
            throw new UnsupportedOperationException();
        }
    }
}
