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

    private final LongBigArray headPointers;
    private final LongBigArray tailPointers;
    private final LongBigArray nextPointers;

    //  A single block can be too huge and cause "Cannot allocate slice larger than 2147483639 bytes"
    private final List<Block> segmentedValues;
    private final PageBuilder pageBuilder;

    private long totalPositions;

    public GroupArrayAggregationState(Type type)
    {
        this.type = type;
        this.headPointers = new LongBigArray(NULL);
        this.tailPointers = new LongBigArray(NULL);
        this.nextPointers = new LongBigArray(NULL);
        this.segmentedValues = new ArrayList<>();
        this.pageBuilder = new PageBuilder(ImmutableList.of(type));

        this.segmentedValues.add(pageBuilder.getBlockBuilder(0));
        totalPositions = 0;
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

        nextPointers.ensureCapacity(totalPositions + 1);
        if (headPointers.get(currentGroupId) == NULL) {
            // new linked list, set up the header pointer
            headPointers.set(currentGroupId, totalPositions);
        }
        else {
            // existing linked list, link the new entry to the tail
            nextPointers.set(tailPointers.get(currentGroupId), totalPositions);
        }
        tailPointers.set(currentGroupId, totalPositions);

        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        type.appendTo(block, position, blockBuilder);
        pageBuilder.declarePosition();
        totalPositions++;

        if (pageBuilder.isFull()) {
            segmentedValues.add(blockBuilder);
            pageBuilder.reset();
        }
    }

    @Override
    public void forEach(ArrayAggregationStateConsumer consumer)
    {
        long currentPosition = headPointers.get(getGroupId());
        while (currentPosition != NULL) {
            // Get block and position in block
            // TODO: Need faster way to do this...
            int blockIndex = 0;
            long positionSum = 0;
            while (segmentedValues.get(blockIndex).getPositionCount() <= currentPosition - positionSum) {
                positionSum += segmentedValues.get(blockIndex).getPositionCount();
                blockIndex++;
            }

            consumer.accept(segmentedValues.get(blockIndex), (int) (currentPosition - positionSum));
            currentPosition = nextPointers.get(currentPosition);
        }
    }

    @Override
    public boolean isEmpty()
    {
        return headPointers.get(getGroupId()) == NULL;
    }
}
