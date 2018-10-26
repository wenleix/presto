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
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamicLifespanScheduler
        implements LifespanScheduler
{
    private final SourceScheduler firstSourceScheduler;

    private final BucketNodeMap bucketNodeMap;
    private final List<ConnectorPartitionHandle> partitionHandles;
    private final OptionalInt concurrentLifespansPerTask;

    private final int[] driverGroupToTaskId;
    private final List<Node> taskIdToNode;
    private final List<IntSet> runningDriverGroupsOnTask;
    private final Queue<Integer> driverGroupScheduleQueue = new ConcurrentLinkedQueue<>();

    private final Set<Lifespan> finishedLifespan = new HashSet<>();

    private boolean initialScheduled;
    private SettableFuture<?> newDriverGroupReady = SettableFuture.create();
    @GuardedBy("this")
    private final List<Lifespan> recentlyCompletedDriverGroups = new ArrayList<>();

    public DynamicLifespanScheduler(
            BucketNodeMap bucketNodeMap,
            List<Node> taskIdToNode,
            List<ConnectorPartitionHandle> partitionHandles,
            OptionalInt concurrentLifespansPerTask,
            SourceScheduler sourceScheduler)
    {
        this.bucketNodeMap = bucketNodeMap;
        this.taskIdToNode = taskIdToNode;
        this.partitionHandles = requireNonNull(partitionHandles, "partitionHandles is null");

        if (concurrentLifespansPerTask.isPresent()) {
            checkArgument(concurrentLifespansPerTask.getAsInt() >= 1, "concurrentLifespansPerTask must be great or equal to 1 if present");
        }
        this.concurrentLifespansPerTask = requireNonNull(concurrentLifespansPerTask, "concurrentLifespansPerTask is null");
        this.firstSourceScheduler = requireNonNull(sourceScheduler, "sourceScheduler is null");

        int bucketCount = partitionHandles.size();
        for (int i = 0; i < bucketCount; i++) {
            driverGroupScheduleQueue.add(i);
        }
        runningDriverGroupsOnTask = new ArrayList<>(taskIdToNode.size());
        for (int i = 0; i < taskIdToNode.size(); i++) {
            runningDriverGroupsOnTask.add(new IntOpenHashSet());
        }
        driverGroupToTaskId = new int[bucketCount];
    }

    @Override
    public void scheduleInitial(SourceScheduler scheduler)
    {
        checkState(!initialScheduled);
        initialScheduled = true;

        int driverGroupsScheduledPerTask = 0;
        while (!driverGroupScheduleQueue.isEmpty()) {
            for (int i = 0; i < taskIdToNode.size() && !driverGroupScheduleQueue.isEmpty(); i++) {
                int driverGroupId = driverGroupScheduleQueue.poll();
                checkState(!bucketNodeMap.getAssignedNode(driverGroupId).isPresent());
                bucketNodeMap.assignOrChangeBucketToNode(driverGroupId, taskIdToNode.get(i));
                runningDriverGroupsOnTask.get(i).add(driverGroupId);
                driverGroupToTaskId[driverGroupId] = i;

                System.err.println("Wenlei Debug: schedule initial " + driverGroupId);
                scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
            }

            driverGroupsScheduledPerTask++;
            if (concurrentLifespansPerTask.isPresent() && driverGroupsScheduledPerTask == concurrentLifespansPerTask.getAsInt()) {
                break;
            }
        }

        if (driverGroupScheduleQueue.isEmpty()) {
            // schedule lifespan can fail and needs to be rescheduled
        //    scheduler.noMoreLifespans();
        }
    }

    @Override
    public void onLifespanFinished(Iterable<Lifespan> newlyCompletedDriverGroups)
    {
        checkState(initialScheduled);

        synchronized (this) {
            for (Lifespan newlyCompletedDriverGroup : newlyCompletedDriverGroups) {
                checkArgument(!newlyCompletedDriverGroup.isTaskWide());

                System.err.println("Wenlei Debug: newly completed lifespan " + newlyCompletedDriverGroup);
                finishedLifespan.add(newlyCompletedDriverGroup);

                int taskId = driverGroupToTaskId[newlyCompletedDriverGroup.getId()];
                runningDriverGroupsOnTask.get(taskId).remove(newlyCompletedDriverGroup.getId());
                recentlyCompletedDriverGroups.add(newlyCompletedDriverGroup);
            }
            newDriverGroupReady.set(null);
        }

        if (finishedLifespan.size() == partitionHandles.size()) {
            System.err.println("Wenlei Debug: oh haha!! all Lifespan finished!!!!!! ");
            firstSourceScheduler.noMoreLifespans();
        }
    }

    public void onTaskFailed(int taskId)
    {
        checkState(initialScheduled);
        checkState(runningDriverGroupsOnTask.get(taskId) != null, "You died for the second time @#^$#@%#@!%#!");

        System.err.println("Wenlei Debug: task Failed!");

        synchronized (this) {
            // Re-add them into the schedule queue
            for (int bucketId : runningDriverGroupsOnTask.get(taskId)) {
                driverGroupScheduleQueue.offer(bucketId);

                System.err.println("Wenlei Debug: requeue lifespan " + bucketId);
            }
            runningDriverGroupsOnTask.set(taskId, null);
            newDriverGroupReady.set(null);
        }
    }

    @Override
    public SettableFuture schedule(SourceScheduler scheduler)
    {
        System.err.println("Wenlei Debug: Try to schedule lifespan in DynamicLifespancScheudluer.schedule!!!");

        // Return a new future even if newDriverGroupReady has not finished.
        // Returning the same SettableFuture instance could lead to ListenableFuture retaining too many listener objects.

        checkState(initialScheduled);

        List<Lifespan> recentlyCompletedDriverGroups;
        synchronized (this) {
            recentlyCompletedDriverGroups = ImmutableList.copyOf(this.recentlyCompletedDriverGroups);
            this.recentlyCompletedDriverGroups.clear();
            newDriverGroupReady = SettableFuture.create();
        }

        synchronized (this) {
            // I don't care who recently complete, who have space, who up~
            for (int i = 0; i < runningDriverGroupsOnTask.size(); i++) {
                if (runningDriverGroupsOnTask.get(i) == null) {
                    // dead...
                    continue;
                }

                while (runningDriverGroupsOnTask.get(i).size() < concurrentLifespansPerTask.getAsInt() && !driverGroupScheduleQueue.isEmpty()) {
                    int driverGroupId = driverGroupScheduleQueue.poll();

                    System.err.println("Wenlei Debug: Schedule in DynamicLifespancScheudluer.schedule: " + driverGroupId);

                    if (bucketNodeMap.getAssignedNode(driverGroupId).isPresent()) {
                        System.err.println("Wenlei Debug: oho, we restart lifespan " + driverGroupId);
                    }

                    bucketNodeMap.assignOrChangeBucketToNode(driverGroupId, taskIdToNode.get(i));
                    runningDriverGroupsOnTask.get(i).add(driverGroupId);
                    driverGroupToTaskId[driverGroupId] = i;

                    // Note, it could mean "restart" lifespan
                    scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
                }
            }

        }

        /*
        for (Lifespan recentlyCompletedDriverGroup : recentlyCompletedDriverGroups) {
            if (driverGroupScheduleQueue.isEmpty()) {
                break;
            }
            int completedDriverGroupId = recentlyCompletedDriverGroup.getId();
            int taskId = driverGroupToTaskId[completedDriverGroupId];
            int driverGroupId = driverGroupScheduleQueue.poll();

            System.err.println("Wenlei Debug: Schedule in DynamicLifespancScheudluer.schedule: " + driverGroupId);

            if (bucketNodeMap.getAssignedNode(driverGroupId).isPresent()) {
                System.err.println("Wenlei Debug: oho, we restart lifespan " + driverGroupId);
            }

            Node nodeForCompletedDriverGroup = bucketNodeMap.getAssignedNode(recentlyCompletedDriverGroup.getId()).get();
            bucketNodeMap.assignOrChangeBucketToNode(driverGroupId, nodeForCompletedDriverGroup);
            runningDriverGroupsOnTask.get(taskId).add(driverGroupId);
            driverGroupToTaskId[driverGroupId] = taskId;

            // TODO: Now, sometimes it means "restart" lifespan...
            scheduler.startLifespan(Lifespan.driverGroup(driverGroupId), partitionHandles.get(driverGroupId));
        }
        */

        if (driverGroupScheduleQueue.isEmpty()) {
            // schedule lifespan can fail and needs to be rescheduled
        //    scheduler.noMoreLifespans();
        }
        return newDriverGroupReady;
    }
}
