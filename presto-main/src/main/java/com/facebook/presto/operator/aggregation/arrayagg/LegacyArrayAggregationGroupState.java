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

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

public class LegacyArrayAggregationGroupState
        extends AbstractGroupedAccumulatorState
        implements ArrayAggregationState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupArrayAggregationState.class).instanceSize();
    private final ObjectBigArray<BlockBuilder> blockBuilders;
    private final Type type;

    public LegacyArrayAggregationGroupState(Type type)
    {
        this.type = type;
        this.blockBuilders = new ObjectBigArray<>();
    }

    @Override
    public void ensureCapacity(long size)
    {
        blockBuilders.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return 0;
    }

    @Override
    public void add(Block block, int position)
    {
        BlockBuilder blockBuilder = blockBuilders.get(getGroupId());
        if (blockBuilder == null) {
            blockBuilder = type.createBlockBuilder(null, 16);
            blockBuilders.set(getGroupId(), blockBuilder);
        }
        type.appendTo(block, position, blockBuilder);
    }

    @Override
    public void forEach(ArrayAggregationStateConsumer consumer)
    {
        BlockBuilder blockBuilder = blockBuilders.get(getGroupId());
        if (blockBuilder == null) {
            return;
        }

        for (int i = 0; i < blockBuilder.getPositionCount(); i++) {
            consumer.accept(blockBuilder, i);
        }
    }

    @Override
    public boolean isEmpty()
    {
        BlockBuilder blockBuilder = blockBuilders.get(getGroupId());
        return blockBuilder == null || blockBuilder.getPositionCount() == 0;
    }

    @Override
    public void reset()
    {
        blockBuilders.set(getGroupId(), null);
    }
}
