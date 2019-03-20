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
package com.facebook.presto.execution.scheduler.group;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.SourceScheduler;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntPriorityQueue;
import it.unimi.dsi.fastutil.ints.IntSet;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

/**
 * See {@link LifespanScheduler} about thread safety
 */
public class DynamicLifespanScheduler
        implements LifespanScheduler
{
    private final static int NOT_ASSIGNED = -1;

    private final BucketNodeMap bucketNodeMap;
    private final List<Node> nodeByTask;
    private final List<ConnectorPartitionHandle> partitionHandles;
    private final OptionalInt concurrentLifespansPerTask;

    private final IntSet[] runningDriverGroupIdsByTask;
    private final int[] taskByDriverGroup;
    private final IntPriorityQueue driverGroupQueue;
    private final IntSet failedTasks;

    // initialScheduled does not need to be guarded because this object
    // is safely published after its mutation.
    private boolean initialScheduled;
    // Write to newDriverGroupReady field is guarded. Read of the reference
    // is either guarded, or is guaranteed to happen in the same thread as the write.
    private SettableFuture<?> newDriverGroupReady = SettableFuture.create();

    @GuardedBy("this")
    private final List<Lifespan> recentlyCompletedDriverGroups = new ArrayList<>();

    public DynamicLifespanScheduler(BucketNodeMap bucketNodeMap, List<Node> nodeByTask, List<ConnectorPartitionHandle> partitionHandles, OptionalInt concurrentLifespansPerTask)
    {
        this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
        this.nodeByTask = requireNonNull(nodeByTask, "nodeByTask is null");
        this.partitionHandles = unmodifiableList(new ArrayList<>(
                requireNonNull(partitionHandles, "partitionHandles is null")));

        this.concurrentLifespansPerTask = requireNonNull(concurrentLifespansPerTask, "concurrentLifespansPerTask is null");
        concurrentLifespansPerTask.ifPresent(lifespansPerTask -> checkArgument(lifespansPerTask >= 1, "concurrentLifespansPerTask must be great or equal to 1 if present"));

        int bucketCount = partitionHandles.size();
        verify(bucketCount > 0);
        this.runningDriverGroupIdsByTask = new IntSet[nodeByTask.size()];
        for (int i = 0; i < nodeByTask.size(); i++) {
            runningDriverGroupIdsByTask[i] = new IntOpenHashSet();
        }
        this.taskByDriverGroup = new int[bucketCount];
        this.driverGroupQueue = new IntArrayFIFOQueue(bucketCount);
        for (int i = 0; i < bucketCount; i++) {
            taskByDriverGroup[i] = NOT_ASSIGNED;
            driverGroupQueue.enqueue(i);
        }
        this.failedTasks = new IntOpenHashSet();
    }

    @Override
    public void scheduleInitial(SourceScheduler scheduler)
    {
        checkState(!initialScheduled);
        initialScheduled = true;

        int driverGroupsScheduledPerTask = 0;
        while (!driverGroupQueue.isEmpty()) {
            for (int i = 0; i < nodeByTask.size() && !driverGroupQueue.isEmpty(); i++) {
                int driverGroupId = driverGroupQueue.dequeueInt();
                checkState(!bucketNodeMap.getAssignedNode(driverGroupId).isPresent());
                bucketNodeMap.assignOrUpdateBucketToNode(driverGroupId, nodeByTask.get(i));
                scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
                runningDriverGroupIdsByTask[i].add(driverGroupId);
            }

            driverGroupsScheduledPerTask++;
            if (concurrentLifespansPerTask.isPresent() && driverGroupsScheduledPerTask == concurrentLifespansPerTask.getAsInt()) {
                break;
            }
        }

        if (driverGroupQueue.isEmpty()) {
            scheduler.noMoreLifespans();
        }
    }

    @Override
    public void onLifespanFinished(Iterable<Lifespan> newlyCompletedDriverGroups)
    {
        checkState(initialScheduled);

        SettableFuture<?> newDriverGroupReady;
        synchronized (this) {
            for (Lifespan newlyCompletedDriverGroup : newlyCompletedDriverGroups) {
                checkArgument(!newlyCompletedDriverGroup.isTaskWide());
                recentlyCompletedDriverGroups.add(newlyCompletedDriverGroup);
                int driverGroupId = newlyCompletedDriverGroup.getId();
                runningDriverGroupIdsByTask[taskByDriverGroup[driverGroupId]].remove(driverGroupId);
            }
            newDriverGroupReady = this.newDriverGroupReady;
        }
        newDriverGroupReady.set(null);
    }

    @Override
    public void retryFailedTask(int taskId)
    {
        checkState(initialScheduled);

        synchronized (this) {
            this.failedTasks.add(taskId);
            for (int driverGroupId : runningDriverGroupIdsByTask[taskId]) {
                driverGroupQueue.enqueue(driverGroupId);
            }
        }
    }

    @Override
    public SettableFuture schedule(SourceScheduler scheduler)
    {
        // Return a new future even if newDriverGroupReady has not finished.
        // Returning the same SettableFuture instance could lead to ListenableFuture retaining too many listener objects.

        checkState(initialScheduled);

        synchronized (this) {
            newDriverGroupReady = SettableFuture.create();
            for (Lifespan driverGroup : recentlyCompletedDriverGroups) {
                if (driverGroupQueue.isEmpty()) {
                    break;
                }
                int driverGroupId = driverGroupQueue.dequeueInt();

                Node nodeForCompletedDriverGroup = bucketNodeMap.getAssignedNode(driverGroup.getId()).orElseThrow(IllegalStateException::new);
                bucketNodeMap.assignOrUpdateBucketToNode(driverGroupId, nodeForCompletedDriverGroup);
                scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
            }
            if (driverGroupQueue.isEmpty()) {
                scheduler.noMoreLifespans();
            }
            this.recentlyCompletedDriverGroups.clear();
        }
        return newDriverGroupReady;
    }
}
