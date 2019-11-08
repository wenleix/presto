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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryPreparer;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.server.QuerySessionSupplier;
import com.facebook.presto.server.SessionContext;
import com.facebook.presto.spark.planner.PreparedPlan;
import com.facebook.presto.spark.planner.SparkPlanFragmenter;
import com.facebook.presto.spark.planner.SparkPlanPreparer;
import com.facebook.presto.spark.planner.SparkQueryPlanner;
import com.facebook.presto.spark.planner.SparkQueryPlanner.PlanAndUpdateType;
import com.facebook.presto.spark.planner.SparkRddPlanner;
import com.facebook.presto.spark.spi.QueryExecution;
import com.facebook.presto.spark.spi.QueryExecutionFactory;
import com.facebook.presto.spark.spi.SessionInfo;
import com.facebook.presto.spark.spi.TaskCompilerFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slices;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.CollectionAccumulator;
import scala.Some;
import scala.Tuple2;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.SystemSessionProperties.isExchangeCompressionEnabled;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PrestoSparkQueryExecutionFactory
        implements QueryExecutionFactory
{
    private final QueryIdGenerator queryIdGenerator;
    private final QuerySessionSupplier sessionSupplier;
    private final QueryPreparer queryPreparer;
    private final SparkQueryPlanner queryPlanner;
    private final SparkPlanFragmenter planFragmenter;
    private final SparkPlanPreparer taskSourceResolver;
    private final SparkRddPlanner rddPlanner;
    private final QueryMonitor queryMonitor;
    private final BlockEncodingManager blockEncodingManager;
    private final JsonCodec<TaskStats> taskStatsJsonCodec;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;

    @Inject
    public PrestoSparkQueryExecutionFactory(
            QueryIdGenerator queryIdGenerator,
            QuerySessionSupplier sessionSupplier,
            QueryPreparer queryPreparer,
            SparkQueryPlanner queryPlanner,
            SparkPlanFragmenter planFragmenter,
            SparkPlanPreparer taskSourceResolver,
            SparkRddPlanner rddPlanner,
            QueryMonitor queryMonitor,
            BlockEncodingManager blockEncodingManager,
            JsonCodec<TaskStats> taskStatsJsonCodec,
            TransactionManager transactionManager,
            AccessControl accessControl)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.queryPlanner = requireNonNull(queryPlanner, "queryPlanner is null");
        this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
        this.taskSourceResolver = requireNonNull(taskSourceResolver, "taskSourceResolver is null");
        this.rddPlanner = requireNonNull(rddPlanner, "rddPlanner is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.blockEncodingManager = requireNonNull(blockEncodingManager, "pagesSerde is null");
        this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public QueryExecution create(SparkContext sparkContext, SessionInfo sessionInfo, String sql, TaskCompilerFactory compiler)
    {
        QueryId queryId = queryIdGenerator.createNextQueryId();
        SessionContext sessionContext = SparkSessionContext.createFromSessionInfo(sessionInfo);
        TransactionId transactionId = transactionManager.beginTransaction(true);
        Session session = sessionSupplier.createSession(queryId, sessionContext)
                .beginTransactionId(transactionId, transactionManager, accessControl);

        // TODO: implement query monitor
        // queryMonitor.queryCreatedEvent();

        // TODO: implement warning collection
        WarningCollector warningCollector = WarningCollector.NOOP;

        PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, sql, warningCollector);
        PlanAndUpdateType planAndUpdateType = queryPlanner.createQueryPlan(session, preparedQuery, warningCollector);
        SubPlan fragmentedPlan = planFragmenter.fragmentQueryPlan(session, planAndUpdateType.getPlan(), warningCollector);
        PreparedPlan preparedPlan = taskSourceResolver.preparePlan(session, fragmentedPlan);

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        CollectionAccumulator<byte[]> taskStatsCollector = new CollectionAccumulator<>();
        taskStatsCollector.register(sparkContext, new Some<>("taskStatsCollector"), false);
        JavaPairRDD<Integer, byte[]> rdd = rddPlanner.createSparkRdd(
                javaSparkContext,
                session,
                preparedPlan,
                compiler,
                taskStatsCollector);

        return new PrestoQueryExecution(
                session,
                queryMonitor,
                taskStatsCollector,
                rdd,
                fragmentedPlan.getFragment().getTypes(),
                planAndUpdateType.getUpdateType(),
                new PagesSerdeFactory(blockEncodingManager, isExchangeCompressionEnabled(session)).createPagesSerde(),
                taskStatsJsonCodec,
                transactionManager);
    }

    public static class PrestoQueryExecution
            implements QueryExecution
    {
        private final Session session;
        private final QueryMonitor queryMonitor;
        private final CollectionAccumulator<byte[]> taskStatsCollector;
        private final JavaPairRDD<Integer, byte[]> rdd;
        private final List<Type> outputTypes;
        private final String updateType;
        private final PagesSerde pagesSerde;
        private final JsonCodec<TaskStats> taskStatsJsonCodec;
        private final TransactionManager transactionManager;

        private PrestoQueryExecution(
                Session session,
                QueryMonitor queryMonitor,
                CollectionAccumulator<byte[]> taskStatsCollector,
                JavaPairRDD<Integer, byte[]> rdd,
                List<Type> outputTypes,
                @Nullable String updateType,
                PagesSerde pagesSerde,
                JsonCodec<TaskStats> taskStatsJsonCodec,
                TransactionManager transactionManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
            this.taskStatsCollector = requireNonNull(taskStatsCollector, "taskStatsCollector is null");
            this.rdd = requireNonNull(rdd, "rdd is null");
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
            this.updateType = updateType;
            this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
            this.taskStatsJsonCodec = requireNonNull(taskStatsJsonCodec, "taskStatsJsonCodec is null");
            this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        }

        @Override
        public List<List<Object>> execute()
        {
            List<Tuple2<Integer, byte[]>> collectedRdd = rdd.collect();
            ConnectorSession connectorSession = session.toConnectorSession();
            List<List<Object>> result = collectedRdd.stream()
                    .map(Tuple2::_2)
                    .map(this::deserializePage)
                    .flatMap(page -> getPageValues(connectorSession, page, outputTypes).stream())
                    .collect(toList());

            List<byte[]> serializedTaskStats = taskStatsCollector.value();
            List<TaskStats> taskStats = serializedTaskStats.stream()
                    .map(taskStatsJsonCodec::fromJson)
                    .collect(toImmutableList());

            // TODO: implement query monitor
            // queryMonitor.queryCompletedEvent();


            // commit transaction
            Optional<TransactionInfo> transaction = session.getTransactionId()
                    .flatMap(transactionManager::getOptionalTransactionInfo);

            checkState(transaction.isPresent() && transaction.get().isAutoCommitContext());
            ListenableFuture<?> commitFuture = transactionManager.asyncCommit(transaction.get().getTransactionId());
            try {
                getFutureValue(commitFuture);
            }
            catch (Throwable t) {
                // TODO: wrap with PrestoException
                throw t;
            }

            return result;
        }

        public List<Type> getOutputTypes()
        {
            return outputTypes;
        }

        public String getUpdateType()
        {
            return updateType;
        }

        private Page deserializePage(byte[] data)
        {
            return pagesSerde.deserialize(readSerializedPages(Slices.wrappedBuffer(data).getInput()).next());
        }

        private static List<List<Object>> getPageValues(ConnectorSession connectorSession, Page page, List<Type> types)
        {
            ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
            for (int position = 0; position < page.getPositionCount(); position++) {
                ImmutableList.Builder<Object> columns = ImmutableList.builder();
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Type type = types.get(channel);
                    Block block = page.getBlock(channel);
                    columns.add(type.getObjectValue(connectorSession, block, position));
                }
                rows.add(columns.build());
            }
            return rows.build();
        }
    }
}
