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
package com.facebook.presto.hive;

import com.facebook.presto.hive.InternalHiveSplit.InternalHiveBlock;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hive.util.AsyncQueue.BorrowResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.NotPartitionedPartitionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;

import java.io.FileNotFoundException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxSplitSize;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.CLOSED;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.FAILED;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.INITIAL;
import static com.facebook.presto.hive.HiveSplitSource.StateKind.NO_MORE_SPLITS;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.failedFuture;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HiveSplit.class);

    private final String queryId;
    private final String databaseName;
    private final String tableName;
    private final TupleDomain<? extends ColumnHandle> compactEffectivePredicate;
    private final PerBucket queues;
    private final AtomicInteger bufferedInternalSplitCount = new AtomicInteger();
    private final int maxOutstandingSplitsBytes;

    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final AtomicInteger remainingInitialSplits;

    private final HiveSplitLoader splitLoader;
    private final AtomicReference<State> stateReference;

    private final AtomicLong estimatedSplitSizeInBytes = new AtomicLong();

    private final CounterStat highMemorySplitSourceCounter;
    private final AtomicBoolean loggedHighMemoryWarning = new AtomicBoolean();

    private HiveSplitSource(
            ConnectorSession session,
            String databaseName,
            String tableName,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            PerBucket queues,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            AtomicReference<State> stateReference,
            CounterStat highMemorySplitSourceCounter)
    {
        requireNonNull(session, "session is null");
        this.queryId = session.getQueryId();
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.queues = requireNonNull(queues, "queues is null");
        this.maxOutstandingSplitsBytes = toIntExact(maxOutstandingSplitsSize.toBytes());
        this.splitLoader = requireNonNull(splitLoader, "splitLoader is null");
        this.stateReference = requireNonNull(stateReference, "stateReference is null");
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");

        this.maxSplitSize = getMaxSplitSize(session);
        this.maxInitialSplitSize = getMaxInitialSplitSize(session);
        this.remainingInitialSplits = new AtomicInteger(maxInitialSplits);
    }

    public static HiveSplitSource allAtOnce(
            ConnectorSession session,
            String databaseName,
            String tableName,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            int maxInitialSplits,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter)
    {
        AtomicReference<State> stateReference = new AtomicReference<>(State.initial());
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                compactEffectivePredicate,
                new PerBucket()
                {
                    private final AsyncQueue<InternalHiveSplit> queue = new AsyncQueue<>(maxOutstandingSplits, executor);

                    @Override
                    public ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit connectorSplit)
                    {
                        // bucketNumber can be non-empty because BackgroundHiveSplitLoader does not have knowledge of execution plan
                        return queue.offer(connectorSplit);
                    }

                    @Override
                    public <O> ListenableFuture<O> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, O>> function)
                    {
                        checkArgument(!bucketNumber.isPresent());
                        return queue.borrowBatchAsync(maxSize, function);
                    }

                    @Override
                    public void finish()
                    {
                        queue.finish();
                    }

                    @Override
                    public boolean isFinished(OptionalInt bucketNumber)
                    {
                        checkArgument(!bucketNumber.isPresent());
                        return queue.isFinished();
                    }

                    @Override
                    public void rewind(OptionalInt bucketNumber)
                    {
                        throw new UnsupportedOperationException();
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                stateReference,
                highMemorySplitSourceCounter);
    }

    public static HiveSplitSource bucketed(
            ConnectorSession session,
            String databaseName,
            String tableName,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter)
    {
        AtomicReference<State> stateReference = new AtomicReference<>(State.initial());
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                compactEffectivePredicate,
                new PerBucket()
                {
                    private final Map<Integer, BucketedSplits> buckets = new HashMap<>();
                    private boolean loadFinished;
                    private final SettableFuture<?> readyForRead = SettableFuture.create();

                    @Override
                    public synchronized ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit internalHiveSplit)
                    {
                        checkState(!loadFinished);
                        BucketedSplits bucketedSplits = splitsFor(bucketNumber);
                        bucketedSplits.allInternalSplits.add(internalHiveSplit);
                        bucketedSplits.queue.add(new InternalHiveSplit(internalHiveSplit));
                        // Do not block "offer" when running split discovery in bucketed mode.
                        // A limit is enforced on estimatedSplitSizeInBytes.
                        return immediateFuture(null);
                    }

                    @Override
                    public synchronized <O> ListenableFuture<O> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, O>> function)
                    {
                        if (!loadFinished) {
                            return readyForRead.transformAsync(ignored -> borrowBatchAsync(bucketNumber, maxSize, function), executor);
                        }

                        BucketedSplits bucketedSplits = splitsFor(bucketNumber);

                        int oldSize = bucketedSplits.queue.size();
                        int reduceBy = Math.min(maxSize, oldSize);
                        if (reduceBy == 0) {
                            BorrowResult<InternalHiveSplit, O> borrowResult = function.apply(ImmutableList.of());
                            checkArgument(borrowResult.getElementsToInsert().isEmpty(), "Function must not insert anything when no element is borrowed");
                            return immediateFuture(borrowResult.getResult());
                        }
                        List<InternalHiveSplit> result = new ArrayList<>(reduceBy);
                        for (int i = 0; i < reduceBy; i++) {
                            result.add(bucketedSplits.queue.remove());
                        }

                        // function deliberately invoked while holding the monitor lock for simplicity
                        BorrowResult<InternalHiveSplit, O> borrowResult = function.apply(result);

                        bucketedSplits.queue.addAll(borrowResult.getElementsToInsert());
                        return immediateFuture(borrowResult.getResult());
                    }

                    @Override
                    public synchronized void finish()
                    {
                        if (loadFinished) {
                            return;
                        }
                        loadFinished = true;
                        readyForRead.set(null);
                    }

                    @Override
                    public synchronized boolean isFinished(OptionalInt bucketNumber)
                    {
                        if (!loadFinished) {
                            return false;
                        }
                        BucketedSplits bucketedSplits = splitsFor(bucketNumber);
                        return bucketedSplits.queue.isEmpty();
                    }

                    @Override
                    public synchronized void rewind(OptionalInt bucketNumber)
                    {
                        checkArgument(bucketNumber.isPresent());

                        // Note: this can also be changed to
                        if (!loadFinished) return;
                        //checkState(loadFinished);

                        BucketedSplits bucketedSplits = splitsFor(bucketNumber);
                        bucketedSplits.queue.clear();

                        for (InternalHiveSplit split : bucketedSplits.allInternalSplits) {
                            bucketedSplits.queue.add(new InternalHiveSplit(split));
                        }
                    }

                    private BucketedSplits splitsFor(OptionalInt bucketNumber)
                    {
                        checkArgument(bucketNumber.isPresent());
                        return buckets.computeIfAbsent(bucketNumber.getAsInt(), ignored -> {
//                            checkState(!loadFinished);
                            return new BucketedSplits();
                        });
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                stateReference,
                highMemorySplitSourceCounter);
    }

    private static class BucketedSplits {
        final List<InternalHiveSplit> allInternalSplits = new ArrayList<>();
        final Queue<InternalHiveSplit> queue = new ArrayDeque<>();
    }

    /**
     * The upper bound of outstanding split count.
     * It might be larger than the actual number when called concurrently with other methods.
     */
    @VisibleForTesting
    int getBufferedInternalSplitCount()
    {
        return bufferedInternalSplitCount.get();
    }

    ListenableFuture<?> addToQueue(List<? extends InternalHiveSplit> splits)
    {
        ListenableFuture<?> lastResult = immediateFuture(null);
        for (InternalHiveSplit split : splits) {
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    ListenableFuture<?> addToQueue(InternalHiveSplit split)
    {
        if (stateReference.get().getKind() != INITIAL) {
            return immediateFuture(null);
        }
        if (estimatedSplitSizeInBytes.addAndGet(split.getEstimatedSizeInBytes()) > maxOutstandingSplitsBytes) {
            // TODO: investigate alternative split discovery strategies when this error is hit.
            // This limit should never be hit given there is a limit of maxOutstandingSplits.
            // If it's hit, it means individual splits are huge.
            if (loggedHighMemoryWarning.compareAndSet(false, true)) {
                highMemorySplitSourceCounter.update(1);
                log.warn("Split buffering for %s.%s in query %s exceeded memory limit (%s). %s splits are buffered.",
                        databaseName, tableName, queryId, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount());
            }
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format(
                    "Split buffering for %s.%s exceeded memory limit (%s). %s splits are buffered.",
                    databaseName, tableName, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount()));
        }
        bufferedInternalSplitCount.incrementAndGet();
        OptionalInt bucketNumber = split.getBucketNumber();
        return queues.offer(bucketNumber, split);
    }

    void noMoreSplits()
    {
        if (setIf(stateReference, State.noMoreSplits(), state -> state.getKind() == INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    void fail(Throwable e)
    {
        // The error must be recorded before setting the finish marker to make sure
        // isFinished will observe failure instead of successful completion.
        // Only record the first error message.
        if (setIf(stateReference, State.failed(e), state -> state.getKind() == INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    @Override
    public void rewind(ConnectorPartitionHandle partitionHandle)
    {
        checkArgument(!(partitionHandle instanceof NotPartitionedPartitionHandle));
        queues.rewind(
                OptionalInt.of(
                        ((HivePartitionHandle) partitionHandle).getBucket()));
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        boolean noMoreSplits;
        State state = stateReference.get();
        switch (state.getKind()) {
            case INITIAL:
                noMoreSplits = false;
                break;
            case NO_MORE_SPLITS:
                noMoreSplits = true;
                break;
            case FAILED:
                return failedFuture(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }

        OptionalInt bucketNumber = toBucketNumber(partitionHandle);
        ListenableFuture<List<ConnectorSplit>> future = queues.borrowBatchAsync(bucketNumber, maxSize, internalSplits -> {
            ImmutableList.Builder<InternalHiveSplit> splitsToInsertBuilder = ImmutableList.builder();
            ImmutableList.Builder<ConnectorSplit> resultBuilder = ImmutableList.builder();
            int removedEstimatedSizeInBytes = 0;
            for (InternalHiveSplit internalSplit : internalSplits) {
                long maxSplitBytes = maxSplitSize.toBytes();
                if (remainingInitialSplits.get() > 0) {
                    if (remainingInitialSplits.getAndDecrement() > 0) {
                        maxSplitBytes = maxInitialSplitSize.toBytes();
                    }
                }
                InternalHiveBlock block = internalSplit.currentBlock();
                long splitBytes;
                if (internalSplit.isSplittable()) {
                    splitBytes = min(maxSplitBytes, block.getEnd() - internalSplit.getStart());
                }
                else {
                    splitBytes = internalSplit.getEnd() - internalSplit.getStart();
                }

                resultBuilder.add(new HiveSplit(
                        databaseName,
                        tableName,
                        internalSplit.getPartitionName(),
                        internalSplit.getPath(),
                        internalSplit.getStart(),
                        splitBytes,
                        internalSplit.getFileSize(),
                        internalSplit.getSchema(),
                        internalSplit.getPartitionKeys(),
                        block.getAddresses(),
                        internalSplit.getBucketNumber(),
                        internalSplit.isForceLocalScheduling(),
                        (TupleDomain<HiveColumnHandle>) compactEffectivePredicate,
                        transformValues(internalSplit.getColumnCoercions(), HiveTypeName::toHiveType),
                        internalSplit.getBucketConversion()));
                internalSplit.increaseStart(splitBytes);

                if (internalSplit.isDone()) {
                    removedEstimatedSizeInBytes += internalSplit.getEstimatedSizeInBytes();
                }
                else {
                    splitsToInsertBuilder.add(internalSplit);
                }
            }
            estimatedSplitSizeInBytes.addAndGet(-removedEstimatedSizeInBytes);

            List<InternalHiveSplit> splitsToInsert = splitsToInsertBuilder.build();
            List<ConnectorSplit> result = resultBuilder.build();
            bufferedInternalSplitCount.addAndGet(splitsToInsert.size() - result.size());

            return new AsyncQueue.BorrowResult<>(splitsToInsert, result);
        });

        ListenableFuture<ConnectorSplitBatch> transform = Futures.transform(future, splits -> {
            requireNonNull(splits, "splits is null");
            if (noMoreSplits) {
                // Checking splits.isEmpty() here is required for thread safety.
                // Let's say there are 10 splits left, and max number of splits per batch is 5.
                // The futures constructed in two getNextBatch calls could each fetch 5, resulting in zero splits left.
                // After fetching the splits, both futures reach this line at the same time.
                // Without the isEmpty check, both will claim they are the last.
                // Side note 1: In such a case, it doesn't actually matter which one gets to claim it's the last.
                //              But having both claim they are the last would be a surprising behavior.
                // Side note 2: One could argue that the isEmpty check is overly conservative.
                //              The caller of getNextBatch will likely need to make an extra invocation.
                //              But an extra invocation likely doesn't matter.
                return new ConnectorSplitBatch(splits, splits.isEmpty() && queues.isFinished(bucketNumber));
            }
            else {
                return new ConnectorSplitBatch(splits, false);
            }
        }, directExecutor());

        return toCompletableFuture(transform);
    }

    @Override
    public boolean isFinished()
    {
        State state = stateReference.get();

        switch (state.getKind()) {
            case INITIAL:
                return false;
            case NO_MORE_SPLITS:
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // This is not working after rewind, since count will be negative!!!!
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                return bufferedInternalSplitCount.get() == 0;
            case FAILED:
                throw propagatePrestoException(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void close()
    {
        if (setIf(stateReference, State.closed(), state -> state.getKind() == INITIAL || state.getKind() == NO_MORE_SPLITS)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    private static OptionalInt toBucketNumber(ConnectorPartitionHandle partitionHandle)
    {
        if (partitionHandle == NOT_PARTITIONED) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(((HivePartitionHandle) partitionHandle).getBucket());
    }

    private static <T> boolean setIf(AtomicReference<T> atomicReference, T newValue, Predicate<T> predicate)
    {
        while (true) {
            T current = atomicReference.get();
            if (!predicate.test(current)) {
                return false;
            }
            if (atomicReference.compareAndSet(current, newValue)) {
                return true;
            }
        }
    }

    private static RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new PrestoException(HIVE_FILE_NOT_FOUND, throwable);
        }
        throw new PrestoException(HIVE_UNKNOWN_ERROR, throwable);
    }

    interface PerBucket
    {
        ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit split);

        <O> ListenableFuture<O> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, O>> function);

        void finish();

        boolean isFinished(OptionalInt bucketNumber);

        void rewind(OptionalInt bucketNumber);
    }

    static class State
    {
        private final StateKind kind;
        private final Throwable throwable;

        private State(StateKind kind, Throwable throwable)
        {
            this.kind = kind;
            this.throwable = throwable;
        }

        public StateKind getKind()
        {
            return kind;
        }

        public Throwable getThrowable()
        {
            checkState(throwable != null);
            return throwable;
        }

        public static State initial()
        {
            return new State(INITIAL, null);
        }

        public static State noMoreSplits()
        {
            return new State(NO_MORE_SPLITS, null);
        }

        public static State failed(Throwable throwable)
        {
            return new State(FAILED, throwable);
        }

        public static State closed()
        {
            return new State(CLOSED, null);
        }
    }

    enum StateKind
    {
        INITIAL,
        NO_MORE_SPLITS,
        FAILED,
        CLOSED,
    }
}
