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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicLifespanScheduler
        implements LifespanScheduler
{
    private final BucketNodeMap bucketNodeMap;
    private final List<Node> allNodes;
    private final List<ConnectorPartitionHandle> partitionHandles;
    private final OptionalInt concurrentLifespansPerTask;

    private final IntListIterator driverGroups;

    private boolean initialScheduled;
    private SettableFuture<?> newDriverGroupReady = SettableFuture.create();
    @GuardedBy("this")
    private final List<Lifespan> recentlyCompletedDriverGroups = new ArrayList<>();

    public DynamicLifespanScheduler(BucketNodeMap bucketNodeMap, List<Node> allNodes, List<ConnectorPartitionHandle> partitionHandles, OptionalInt concurrentLifespansPerTask)
    {
        this.bucketNodeMap = bucketNodeMap;
        this.allNodes = allNodes;
        this.partitionHandles = requireNonNull(partitionHandles, "partitionHandles is null");

        if (concurrentLifespansPerTask.isPresent()) {
            checkArgument(concurrentLifespansPerTask.getAsInt() >= 1, "concurrentLifespansPerTask must be great or equal to 1 if present");
        }
        this.concurrentLifespansPerTask = requireNonNull(concurrentLifespansPerTask, "concurrentLifespansPerTask is null");

        int bucketCount = partitionHandles.size();
        this.driverGroups = new IntArrayList(IntStream.range(0, bucketCount).toArray()).iterator();
    }

    @Override
    public void scheduleInitial(SourceScheduler scheduler)
    {
        checkState(!initialScheduled);
        initialScheduled = true;

        int driverGroupsScheduledPerTask = 0;
        while (driverGroups.hasNext()) {
            for (int i = 0; i < allNodes.size() && driverGroups.hasNext(); i++) {
                int driverGroupId = driverGroups.nextInt();
                checkState(!bucketNodeMap.getAssignedNode(driverGroupId).isPresent());
                bucketNodeMap.assignOrChangeBucketToNode(driverGroupId, allNodes.get(i));
                scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
            }

            driverGroupsScheduledPerTask++;
            if (concurrentLifespansPerTask.isPresent() && driverGroupsScheduledPerTask == concurrentLifespansPerTask.getAsInt()) {
                break;
            }
        }

        if (!driverGroups.hasNext()) {
            scheduler.noMoreLifespans();
        }
    }

    @Override
    public void onLifespanFinished(Iterable<Lifespan> newlyCompletedDriverGroups)
    {
        checkState(initialScheduled);

        synchronized (this) {
            for (Lifespan newlyCompletedDriverGroup : newlyCompletedDriverGroups) {
                checkArgument(!newlyCompletedDriverGroup.isTaskWide());
                recentlyCompletedDriverGroups.add(newlyCompletedDriverGroup);
            }
            newDriverGroupReady.set(null);
        }
    }

    @Override
    public SettableFuture schedule(SourceScheduler scheduler)
    {
        // Return a new future even if newDriverGroupReady has not finished.
        // Returning the same SettableFuture instance could lead to ListenableFuture retaining too many listener objects.

        checkState(initialScheduled);

        List<Lifespan> recentlyCompletedDriverGroups;
        synchronized (this) {
            recentlyCompletedDriverGroups = ImmutableList.copyOf(this.recentlyCompletedDriverGroups);
            this.recentlyCompletedDriverGroups.clear();
            newDriverGroupReady = SettableFuture.create();
        }

        for (Lifespan driverGroup : recentlyCompletedDriverGroups) {
            if (!driverGroups.hasNext()) {
                break;
            }
            int driverGroupId = driverGroups.nextInt();
            checkState(!bucketNodeMap.getAssignedNode(driverGroupId).isPresent());

            Node nodeForCompletedDriverGroup = bucketNodeMap.getAssignedNode(driverGroup.getId()).get();
            bucketNodeMap.assignOrChangeBucketToNode(driverGroupId, nodeForCompletedDriverGroup);
            scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
        }

        if (!driverGroups.hasNext()) {
            scheduler.noMoreLifespans();
        }
        return newDriverGroupReady;
    }
}
