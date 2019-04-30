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
package com.facebook.presto.operator.aggregation.arbitrary;

import com.facebook.presto.array.IntBigArray;
import com.facebook.presto.array.ShortBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Verify.verify;

/**
 * state object that uses a single BlockBuilder for all groups.
 */
public class GroupArbitraryAggregationState
        extends AbstractGroupedAccumulatorState
        implements ArbitraryAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupArbitraryAggregationState.class).instanceSize();
    private static final int MAX_BLOCK_SIZE = 1024 * 1024;
    private static final int MAX_NUM_BLOCKS = 30000;
    private static final short NULL = -1;

    private final Type type;

    private final ShortBigArray headBlockIndex;
    private final IntBigArray headPosition;

    private final List<BlockBuilder> values;
    private BlockBuilder currentBlockBuilder;
    private PageBuilderStatus pageBuilderStatus;

    private long valueBlocksRetainedSizeInBytes;

    public GroupArbitraryAggregationState(Type type)
    {
        this.type = type;
        this.headBlockIndex = new ShortBigArray(NULL);
        this.headPosition = new IntBigArray(NULL);

        this.pageBuilderStatus = new PageBuilderStatus(MAX_BLOCK_SIZE);
        this.currentBlockBuilder = type.createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), 16);
        this.values = new ArrayList<>();
        values.add(currentBlockBuilder);
        valueBlocksRetainedSizeInBytes = 0;
    }

    @Override
    public void ensureCapacity(long size)
    {
        headBlockIndex.ensureCapacity(size);
        headPosition.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                headBlockIndex.sizeOf() +
                headPosition.sizeOf() +
                valueBlocksRetainedSizeInBytes +
                // valueBlocksRetainedSizeInBytes doesn't contain the current block builder
                currentBlockBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void add(Block block, int position)
    {
        if (!isEmpty()) {
            return;
        }

        long currentGroupId = getGroupId();
        short insertedBlockIndex = (short) (values.size() - 1);
        int insertedPosition = currentBlockBuilder.getPositionCount();

        // new linked list, set up the header pointer
        headBlockIndex.set(currentGroupId, insertedBlockIndex);
        headPosition.set(currentGroupId, insertedPosition);

        type.appendTo(block, position, currentBlockBuilder);

        if (pageBuilderStatus.isFull()) {
            valueBlocksRetainedSizeInBytes += currentBlockBuilder.getRetainedSizeInBytes();
            pageBuilderStatus = new PageBuilderStatus(MAX_BLOCK_SIZE);
            currentBlockBuilder = currentBlockBuilder.newBlockBuilderLike(pageBuilderStatus.createBlockBuilderStatus());
            values.add(currentBlockBuilder);

            verify(values.size() <= MAX_NUM_BLOCKS);
        }
    }

    @Override
    public void forEach(ArbitraryAggregationStateConsumer consumer)
    {
        short currentBlockId = headBlockIndex.get(getGroupId());
        int currentPosition = headPosition.get(getGroupId());
        if (currentBlockId != NULL) {
            consumer.accept(values.get(currentBlockId), currentPosition);
        }
    }

    @Override
    public boolean isEmpty()
    {
        return headBlockIndex.get(getGroupId()) == NULL;
    }
}