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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.OperationTimer.OperationTiming;
import com.facebook.presto.operator.TableWriterOperator.TableWriterContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static com.facebook.presto.operator.TableWriterOperator.CONTEXT_CHANNEL;
import static com.facebook.presto.operator.TableWriterOperator.FRAGMENT_CHANNEL;
import static com.facebook.presto.operator.TableWriterOperator.ROW_COUNT_CHANNEL;
import static com.facebook.presto.operator.TableWriterOperator.TableWriterContext.TABLE_WRITER_CONTEXT_JSON_CODEC;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.Objects.requireNonNull;
import static org.glassfish.jersey.internal.util.collection.ImmutableCollectors.toImmutableList;

public class TableFinishOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.of(BIGINT);

    public static class TableFinishOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final TableFinisher tableFinisher;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final StatisticAggregationsDescriptor<Integer> descriptor;
        private final Session session;
        private boolean closed;

        public TableFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                TableFinisher tableFinisher,
                OperatorFactory statisticsAggregationOperatorFactory,
                StatisticAggregationsDescriptor<Integer> descriptor,
                Session session)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.tableFinisher = requireNonNull(tableFinisher, "tableFinisher is null");
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableFinishOperator.class.getSimpleName());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean statisticsCpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            return new TableFinishOperator(context, tableFinisher, statisticsAggregationOperator, descriptor, statisticsCpuTimerEnabled);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableFinishOperatorFactory(operatorId, planNodeId, tableFinisher, statisticsAggregationOperatorFactory, descriptor, session);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final TableFinisher tableFinisher;
    private final Operator statisticsAggregationOperator;
    private final StatisticAggregationsDescriptor<Integer> descriptor;

    private State state = State.RUNNING;
    private long rowCount;
    private Optional<ConnectorOutputMetadata> outputMetadata = Optional.empty();
    private final ImmutableList.Builder<Slice> fragmentBuilder = ImmutableList.builder();
    private final ImmutableList.Builder<ComputedStatistics> computedStatisticsBuilder = ImmutableList.builder();

    private final OperationTiming statisticsTiming = new OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    private final GroupStageStateTracker groupStageStateTracker = new GroupStageStateTracker();

    public TableFinishOperator(
            OperatorContext operatorContext,
            TableFinisher tableFinisher,
            Operator statisticsAggregationOperator,
            StatisticAggregationsDescriptor<Integer> descriptor,
            boolean statisticsCpuTimerEnabled)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.tableFinisher = requireNonNull(tableFinisher, "tableCommitter is null");
        this.statisticsAggregationOperator = requireNonNull(statisticsAggregationOperator, "statisticsAggregationOperator is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;

        operatorContext.setInfoSupplier(this::getInfo);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        statisticsAggregationOperator.finish();
        timer.end(statisticsTiming);

        if (state == State.RUNNING) {
            state = State.FINISHING;
        }
    }

    @Override
    public boolean isFinished()
    {
        if (state == State.FINISHED) {
            verify(statisticsAggregationOperator.isFinished());
            return true;
        }
        return false;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return statisticsAggregationOperator.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        if (state != State.RUNNING) {
            return false;
        }
        return statisticsAggregationOperator.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        groupStageStateTracker.update(page);
        groupStageStateTracker.getStatisticsPagesToProcess(page).forEach(statisticsPage -> {
            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            statisticsAggregationOperator.addInput(statisticsPage);
            timer.end(statisticsTiming);
        });
    }

    private static Optional<Page> extractStatisticsRows(Page page)
    {
        int statisticsPositionCount = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (isStatisticsPosition(page, position)) {
                statisticsPositionCount++;
            }
        }

        if (statisticsPositionCount == 0) {
            return Optional.empty();
        }

        if (statisticsPositionCount == page.getPositionCount()) {
            return Optional.of(page);
        }

        int selectedPositionsIndex = 0;
        int[] selectedPositions = new int[statisticsPositionCount];
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (isStatisticsPosition(page, position)) {
                selectedPositions[selectedPositionsIndex] = position;
                selectedPositionsIndex++;
            }
        }

        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            blocks[channel] = page.getBlock(channel).getPositions(selectedPositions, 0, statisticsPositionCount);
        }
        return Optional.of(new Page(statisticsPositionCount, blocks));
    }

    /**
     * Both the statistics and the row_count + fragments are transferred over the same communication
     * link between the TableWriterOperator and the TableFinishOperator. Thus the multiplexing is needed.
     * <p>
     * The transferred page layout looks like:
     * <p>
     * [[row_count_channel], [fragment_channel], [statistic_channel_1] ... [statistic_channel_N]]
     * <p>
     * [row_count_channel] - contains number of rows processed by a TableWriterOperator instance
     * [fragment_channel] - contains arbitrary binary data provided by the ConnectorPageSink#finish for
     * the further post processing on the coordinator
     * <p>
     * [statistic_channel_1] ... [statistic_channel_N] - contain pre-aggregated statistics computed by the
     * statistics aggregation operator within the
     * TableWriterOperator
     * <p>
     * Since the final aggregation operator in the TableFinishOperator doesn't know what to do with the
     * first two channels, those must be pruned. For the convenience we never set both, the
     * [row_count_channel] + [fragment_channel] and the [statistic_channel_1] ... [statistic_channel_N].
     * <p>
     * If this is a row that holds statistics - the [row_count_channel] + [fragment_channel] will be NULL.
     * <p>
     * It this is a row that holds the row count or the fragment - all the statistics channels will be set
     * to NULL.
     * <p>
     * Since neither [row_count_channel] or [fragment_channel] cannot hold the NULL value naturally, by
     * checking isNull on these two channels we can determine if this is a row that contains statistics.
     */
    private static boolean isStatisticsPosition(Page page, int position)
    {
        return page.getBlock(ROW_COUNT_CHANNEL).isNull(position) && page.getBlock(FRAGMENT_CHANNEL).isNull(position);
    }

    @Override
    public Page getOutput()
    {
        if (!isBlocked().isDone()) {
            return null;
        }

        if (!statisticsAggregationOperator.isFinished()) {
            verify(statisticsAggregationOperator.isBlocked().isDone(), "aggregation operator should not be blocked");

            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            Page page = statisticsAggregationOperator.getOutput();
            timer.end(statisticsTiming);

            if (page == null) {
                return null;
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                computedStatisticsBuilder.add(getComputedStatistics(page, position));
            }
            return null;
        }

        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;
        groupStageStateTracker.finish();

        outputMetadata = tableFinisher.finishTable(groupStageStateTracker.getFragments(), computedStatisticsBuilder.build());

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(1, TYPES);
        page.declarePosition();
        BIGINT.writeLong(page.getBlockBuilder(0), groupStageStateTracker.getRowCount());
        return page.build();
    }

    private ComputedStatistics getComputedStatistics(Page page, int position)
    {
        ImmutableList.Builder<String> groupingColumns = ImmutableList.builder();
        ImmutableList.Builder<Block> groupingValues = ImmutableList.builder();
        descriptor.getGrouping().forEach((column, channel) -> {
            groupingColumns.add(column);
            groupingValues.add(page.getBlock(channel).getSingleValueBlock(position));
        });

        ComputedStatistics.Builder statistics = ComputedStatistics.builder(groupingColumns.build(), groupingValues.build());

        descriptor.getTableStatistics().forEach((type, channel) ->
                statistics.addTableStatistic(type, page.getBlock(channel).getSingleValueBlock(position)));

        descriptor.getColumnStatistics().forEach((metadata, channel) -> statistics.addColumnStatistic(metadata, page.getBlock(channel).getSingleValueBlock(position)));

        return statistics.build();
    }

    @VisibleForTesting
    TableFinishInfo getInfo()
    {
        return new TableFinishInfo(
                outputMetadata,
                succinctNanos(statisticsTiming.getWallNanos()),
                succinctNanos(statisticsTiming.getCpuNanos()));
    }

    @Override
    public void close()
            throws Exception
    {
        statisticsAggregationOperator.close();
    }

    public interface TableFinisher
    {
        Optional<ConnectorOutputMetadata> finishTable(Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);
    }

    public interface LifespanCommitter
    {
        void commitLifespan(Collection<Slice> fragments);
    }

    private static class GroupStageStateTracker
    {
        private final Map<LifespanStage, LifespanStageState> taskWideLifespanStageStates = new HashMap<>();
        private final Map<LifespanStage, Map<Integer, LifespanStageState>> unfinishedGroupedLifespanStageStates = new HashMap<>();
        private final Map<LifespanStage, LifespanStageState> finishedLifespanStages = new HashMap<>();

        public void update(Page page)
        {
            TableWriterContext tableWriterContext = getTableWriterContext(page);
            LifespanStage lifespanStage = LifespanStage.fromTableWriterContext(tableWriterContext);
            if (finishedLifespanStages.containsKey(lifespanStage)) {
                return;
            }

            if (lifespanStage.getLifespan().isTaskWide()) {
                taskWideLifespanStageStates.putIfAbsent(lifespanStage, new LifespanStageState());
                taskWideLifespanStageStates.get(lifespanStage).update(page, false);
            }
            else {
                unfinishedGroupedLifespanStageStates.putIfAbsent(lifespanStage, new HashMap<>());
                Map<Integer, LifespanStageState> groupStageStatesPerTask = unfinishedGroupedLifespanStageStates.get(lifespanStage);
                groupStageStatesPerTask.putIfAbsent(tableWriterContext.getTaskId(), new LifespanStageState());
                groupStageStatesPerTask.get(tableWriterContext.getTaskId()).update(page, true);

                if (isLastPageForGroupedLifespan(page)) {
                    checkState(!finishedLifespanStages.containsKey(lifespanStage), "GroupStage already finished");
                    finishedLifespanStages.put(lifespanStage, groupStageStatesPerTask.get(tableWriterContext.getTaskId()));
                    unfinishedGroupedLifespanStageStates.remove(lifespanStage);
                }
            }
        }

        public List<Page> getStatisticsPagesToProcess(Page page)
        {
            LifespanStage lifespanStage = LifespanStage.fromTableWriterContext(getTableWriterContext(page));
            if (lifespanStage.getLifespan().isTaskWide()) {
                return extractStatisticsRows(page).map(ImmutableList::of).orElse(ImmutableList.of());
            }
            if (!finishedLifespanStages.containsKey(lifespanStage)) {
                return ImmutableList.of();
            }
            return finishedLifespanStages.get(lifespanStage).getStatisticsPages();
        }

        public void finish()
        {
            for (LifespanStage lifespanStage : unfinishedGroupedLifespanStageStates.keySet()) {
                checkState(lifespanStage.getLifespan().isTaskWide(), "Only task wide lifespan needs explicit finish");
                finishedLifespanStages.put(lifespanStage, getOnlyElement(unfinishedGroupedLifespanStageStates.get(lifespanStage).values()));
            }
        }

        public long getRowCount()
        {
            return finishedLifespanStages.values().stream()
                    .mapToLong(LifespanStageState::getRowCount)
                    .sum();
        }

        public List<Slice> getFragments()
        {
            return finishedLifespanStages.values().stream()
                    .map(LifespanStageState::getFragments)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }

        private static TableWriterContext getTableWriterContext(Page page)
        {
            Block tableWriterContextBlock = page.getBlock(CONTEXT_CHANNEL);
            return TABLE_WRITER_CONTEXT_JSON_CODEC.fromJson(tableWriterContextBlock.getSlice(0, 0, tableWriterContextBlock.getSliceLength(0)).getBytes());
        }

        private static boolean isLastPageForGroupedLifespan(Page page)
        {
            return !page.getBlock(ROW_COUNT_CHANNEL).isNull(0);
        }

        private static class LifespanStage
        {
            private final Lifespan lifespan;
            private final int stageId;

            private LifespanStage(Lifespan lifespan, int stageId)
            {
                this.lifespan = requireNonNull(lifespan, "lifespan is null");
                this.stageId = stageId;
            }

            public static LifespanStage fromTableWriterContext(TableWriterContext tableWriterContext)
            {
                return new LifespanStage(tableWriterContext.getLifespan(), tableWriterContext.getStageId());
            }

            public Lifespan getLifespan()
            {
                return lifespan;
            }

            public int getStageId()
            {
                return stageId;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof LifespanStage)) {
                    return false;
                }
                LifespanStage that = (LifespanStage) o;
                return stageId == that.stageId &&
                        Objects.equals(lifespan, that.lifespan);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(lifespan, stageId);
            }

            @Override
            public String toString()
            {
                return MoreObjects.toStringHelper(this)
                        .add("lifespan", lifespan)
                        .add("stageId", stageId)
                        .toString();
            }
        }

        private static class LifespanStageState
        {
            private long rowCount;
            private ImmutableList.Builder<Slice> fragmentBuilder = ImmutableList.builder();
            private ImmutableList.Builder<Page> statisticsPages = ImmutableList.builder();

            public void update(Page page, boolean storeStatisticPages)
            {
                Block rowCountBlock = page.getBlock(ROW_COUNT_CHANNEL);
                Block fragmentBlock = page.getBlock(FRAGMENT_CHANNEL);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (!rowCountBlock.isNull(position)) {
                        rowCount += BIGINT.getLong(rowCountBlock, position);
                    }
                    if (!fragmentBlock.isNull(position)) {
                        fragmentBuilder.add(VARBINARY.getSlice(fragmentBlock, position));
                    }
                }
                if (storeStatisticPages) {
                    extractStatisticsRows(page).ifPresent(statisticsPages::add);
                }
            }

            public long getRowCount()
            {
                return rowCount;
            }

            public List<Slice> getFragments()
            {
                return fragmentBuilder.build();
            }

            public List<Page> getStatisticsPages()
            {
                return statisticsPages.build();
            }
        }
    }
}
