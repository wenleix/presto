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
package com.facebook.presto.operator.aggregation.arrayagg;

import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * state object that uses a single BlockBuilder for all groups.
 */
public class GroupArrayAggregationState
        extends AbstractGroupedAccumulatorState
        implements ArrayAggregationState
{
    private static final long NULL = -1;

    private final Type type;

    private LongBigArray headPointers;
    private LongBigArray tailPointers;
    private LongBigArray nextDeltaPointers;   // TODO: Try to use int

    //  A single block can be too huge and cause "Cannot allocate slice larger than 2147483639 bytes"
    private List<Block> segmentedValues;
    private PageBuilder pageBuilder;

    private long totalPositions;
    private long capacity;

    public GroupArrayAggregationState(Type type)
    {
        this.type = type;
        this.headPointers = new LongBigArray(NULL);
        this.tailPointers = new LongBigArray(NULL);
        this.nextDeltaPointers = new LongBigArray(NULL);
        this.segmentedValues = new ArrayList<>();
        this.pageBuilder = new PageBuilder(ImmutableList.of(type));

        this.segmentedValues.add(pageBuilder.getBlockBuilder(0));
        totalPositions = 0;
        capacity = 1024;
        nextDeltaPointers.ensureCapacity(capacity);
    }

    @Override
    public void ensureCapacity(long size)
    {
        headPointers.ensureCapacity(size);
        tailPointers.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return 0;
    }

    @Override
    public void add(Block block, int position)
    {
        long currentGroupId = getGroupId();

        if (totalPositions == capacity) {
            capacity *= 1.5;
            nextDeltaPointers.ensureCapacity(capacity);
        }

        if (headPointers.get(currentGroupId) == NULL) {
            // new linked list, set up the header pointer
            headPointers.set(currentGroupId, totalPositions);
        }
        else {
            // existing linked list, link the new entry to the tail
            long tailPosition = tailPointers.get(currentGroupId);
            nextDeltaPointers.set(tailPosition, totalPositions - tailPosition);
        }
        tailPointers.set(currentGroupId, totalPositions);

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        type.appendTo(block, position, blockBuilder);
        pageBuilder.declarePosition();
        totalPositions++;

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
            segmentedValues.add(pageBuilder.getBlockBuilder(0));
        }
    }

    @Override
    public void forEach(ArrayAggregationStateConsumer consumer)
    {
        long currentGlobalPosition = headPointers.get(getGroupId());
        int segmentId = 0;
        long localPosition = currentGlobalPosition;

        while (true) {
            // Get block and position in segments
            while (true) {
                int currentSegmentPositionCount = segmentedValues.get(segmentId).getPositionCount();
                if (currentSegmentPositionCount > localPosition) {
                    break;
                }
                localPosition -= currentSegmentPositionCount;
                segmentId++;
            }
            consumer.accept(segmentedValues.get(segmentId), (int) (localPosition));

            long delta = nextDeltaPointers.get(currentGlobalPosition);
            if (delta == NULL) {
                break;
            }
            currentGlobalPosition += delta;
            localPosition += delta;
        }
    }

    @Override
    public boolean isEmpty()
    {
        return headPointers.get(getGroupId()) == NULL;
    }

    @Override
    public void reset()
    {
        // TODO: only reset this..
        headPointers = new LongBigArray(NULL);
        tailPointers = new LongBigArray(NULL);
        nextDeltaPointers = new LongBigArray(NULL);
        segmentedValues = new ArrayList<>();
        pageBuilder = new PageBuilder(ImmutableList.of(type));

        segmentedValues.add(pageBuilder.getBlockBuilder(0));
        totalPositions = 0;
        capacity = 1024;
        nextDeltaPointers.ensureCapacity(capacity);
    }
}
