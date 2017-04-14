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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.array.BlockBigArray;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

public class ReduceAggregationStateFactory
        implements AccumulatorStateFactory<ReduceAggregationState>
{
    @Override
    public ReduceAggregationState createSingleState()
    {
        return new SingleReduceAggregationState();
    }

    @Override
    public Class<? extends ReduceAggregationState> getSingleStateClass()
    {
        return SingleReduceAggregationState.class;
    }

    @Override
    public ReduceAggregationState createGroupedState()
    {
        return new SingleReduceAggregationState();
    }

    @Override
    public Class<? extends ReduceAggregationState> getGroupedStateClass()
    {
        return GroupedReduceAggregationState.class;
    }

    public static class GroupedReduceAggregationState
            extends AbstractGroupedAccumulatorState
            implements ReduceAggregationState
    {
        private final BlockBigArray states = new BlockBigArray();

        @Override
        public void ensureCapacity(long size)
        {
            states.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return states.sizeOf();
        }

        @Override
        public Block getState()
        {
            return states.get(getGroupId());
        }

        @Override
        public void setState(Block state)
        {
            states.set(getGroupId(), state);
        }
    }

    public static class SingleReduceAggregationState
            implements ReduceAggregationState
    {
        private Block state;

        @Override
        public long getEstimatedSize()
        {
            return state == null ? 0 : state.getRetainedSizeInBytes();
        }

        @Override
        public Block getState()
        {
            return state;
        }

        @Override
        public void setState(Block state)
        {
            this.state = state;
        }
    }
}
