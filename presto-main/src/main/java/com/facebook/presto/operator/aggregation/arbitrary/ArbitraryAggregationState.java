package com.facebook.presto.operator.aggregation.arbitrary;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.type.Type;

public interface ArbitraryAggregationState
        extends AccumulatorState
{
    void add(Block block, int position);

    void forEach(ArbitraryAggregationStateConsumer consumer);

    boolean isEmpty();

    default void merge(ArbitraryAggregationState otherState)
    {
        otherState.forEach(this::add);
    }

    default void reset()
    {
        throw new UnsupportedOperationException();
    }

    static void write(Type type, ArbitraryAggregationState state, BlockBuilder out)
    {
        state.forEach((block, position) -> type.appendTo(block, position, out));
    }
}
