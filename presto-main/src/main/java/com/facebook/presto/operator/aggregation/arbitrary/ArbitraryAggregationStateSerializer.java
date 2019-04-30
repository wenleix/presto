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

import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggregationState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;

public class ArbitraryAggregationStateSerializer
        implements AccumulatorStateSerializer<ArbitraryAggregationState>
{
    private final Type elementType;

    public ArbitraryAggregationStateSerializer(Type elementType)
    {
        this.elementType = elementType;
    }

    @Override
    public Type getSerializedType()
    {
        return elementType;
    }

    @Override
    public void serialize(ArbitraryAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            state.forEach((block, position) -> elementType.appendTo(block, position, out));
        }
    }

    @Override
    public void deserialize(Block block, int index, ArbitraryAggregationState state)
    {
        state.reset();
        state.add(block, index);
    }
}
