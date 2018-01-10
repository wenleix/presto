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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;

public class ReduceAggregationStateSerializer
        implements AccumulatorStateSerializer<ReduceAggregationState>
{
    private final Type stateType;

    public ReduceAggregationStateSerializer(Type stateType)
    {
        this.stateType = stateType;
    }

    @Override
    public Type getSerializedType()
    {
        return stateType;
    }

    @Override
    public void serialize(ReduceAggregationState aggregationState, BlockBuilder out)
    {
        Block state = aggregationState.getState();

        if (state == null) {
            out.appendNull();
            return;
        }

        stateType.appendTo(state, 0, out);
    }

    @Override
    public void deserialize(Block block, int index, ReduceAggregationState state)
    {
        state.setState(block.getSingleValueBlock(index));
    }
}
