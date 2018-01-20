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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.aggregation.lambda.LambdaChannelProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.WindowIndex;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Long.max;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class GenericAccumulatorFactory
        implements AccumulatorFactory
{
    private final AccumulatorStateSerializer<?> stateSerializer;
    private final AccumulatorStateFactory<?> stateFactory;
    private final Constructor<? extends Accumulator> accumulatorConstructor;
    private final Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor;
    private final Optional<Integer> maskChannel;
    private final List<MethodHandle> lambdaChannelProviderFactory;
    private final List<Integer> inputChannels;
    private final List<Type> sourceTypes;
    private final List<Integer> orderByChannels;
    private final List<SortOrder> orderings;
    private final PagesIndex.Factory pagesIndexFactory;

    public GenericAccumulatorFactory(
            AccumulatorStateSerializer<?> stateSerializer,
            AccumulatorStateFactory<?> stateFactory,
            Constructor<? extends Accumulator> accumulatorConstructor,
            Constructor<? extends GroupedAccumulator> groupedAccumulatorConstructor,
            List<Integer> inputChannels,
            Optional<Integer> maskChannel,
            List<MethodHandle> lambdaChannelProviderFactory,
            List<Type> sourceTypes,
            List<Integer> orderByChannels,
            List<SortOrder> orderings,
            PagesIndex.Factory pagesIndexFactory)
    {
        this.stateSerializer = requireNonNull(stateSerializer, "stateSerializer is null");
        this.stateFactory = requireNonNull(stateFactory, "stateFactory is null");
        this.accumulatorConstructor = requireNonNull(accumulatorConstructor, "accumulatorConstructor is null");
        this.groupedAccumulatorConstructor = requireNonNull(groupedAccumulatorConstructor, "groupedAccumulatorConstructor is null");
        this.maskChannel = requireNonNull(maskChannel, "maskChannel is null");
        this.lambdaChannelProviderFactory = requireNonNull(lambdaChannelProviderFactory, "lambdaChannelProviderFactory is null");
        this.inputChannels = ImmutableList.copyOf(requireNonNull(inputChannels, "inputChannels is null"));
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
        this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        checkArgument(orderByChannels.isEmpty() || !isNull(pagesIndexFactory), "No pagesIndexFactory to process ordering");
        this.pagesIndexFactory = pagesIndexFactory;
    }

    @Override
    public List<Integer> getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Accumulator createAccumulator()
    {
        Accumulator accumulator;
        try {
            accumulator = accumulatorConstructor.newInstance(stateSerializer, stateFactory, inputChannels, maskChannel);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        if (orderByChannels.isEmpty()) {
            return accumulator;
        }

        return new OrderingAccumulator(accumulator, sourceTypes, orderByChannels, orderings, pagesIndexFactory);
    }

    @Override
    public Accumulator createIntermediateAccumulator()
    {
        try {
            return accumulatorConstructor.newInstance(stateSerializer, stateFactory, ImmutableList.of(), Optional.empty());
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    // We can either add connector session here, or add into addInput() functions...
    @Override
    public GroupedAccumulator createGroupedAccumulator()
    {
        GroupedAccumulator accumulator;
        try {
            accumulator = groupedAccumulatorConstructor.newInstance(stateSerializer, stateFactory, inputChannels, maskChannel, getLambdaChannelProviders());
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }

        if (orderByChannels.isEmpty()) {
            return accumulator;
        }

        return new OrderingGroupedAccumulator(accumulator, sourceTypes, orderByChannels, orderings, pagesIndexFactory);
    }

    @Override
    public GroupedAccumulator createGroupedIntermediateAccumulator()
    {
        try {
            return groupedAccumulatorConstructor.newInstance(stateSerializer, stateFactory, ImmutableList.of(), maskChannel, getLambdaChannelProviders());
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public List<LambdaChannelProvider> getLambdaChannelProviders()
    {
        ImmutableList.Builder<LambdaChannelProvider> builder = ImmutableList.builder();
        for (MethodHandle factory : lambdaChannelProviderFactory) {
            try {
                // This assumes connector session needs to be provided for the constructor of LambdaChannelProvider
                builder.add((LambdaChannelProvider) factory.invoke(null));
            }
            catch (Throwable throwable) {
                Throwables.propagate(throwable);
            }
        }
        return builder.build();
    }

    @Override
    public boolean hasOrderBy()
    {
        return !orderByChannels.isEmpty();
    }

    private static class OrderingAccumulator
            implements Accumulator
    {
        private final Accumulator accumulator;
        private final List<Integer> orderByChannels;
        private final List<SortOrder> orderings;
        private final PagesIndex pagesIndex;

        private OrderingAccumulator(
                Accumulator accumulator,
                List<Type> aggregationSourceTypes,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                PagesIndex.Factory pagesIndexFactory)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
            this.pagesIndex = pagesIndexFactory.newPagesIndex(aggregationSourceTypes, 10_000);
        }

        @Override
        public long getEstimatedSize()
        {
            return pagesIndex.getEstimatedSize().toBytes() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addInput(ConnectorSession session, Page page)
        {
            pagesIndex.addPage(page);
        }

        @Override
        public void addInput(WindowIndex index, List<Integer> channels, int startPosition, int endPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addIntermediate(ConnectorSession session, Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(BlockBuilder blockBuilder)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(BlockBuilder blockBuilder)
        {
            pagesIndex.sort(orderByChannels, orderings);
            Iterator<Page> pagesIterator = pagesIndex.getSortedPages();
            // hack!!!
            pagesIterator.forEachRemaining(page -> accumulator.addInput(null, page));
            accumulator.evaluateFinal(blockBuilder);
        }
    }

    private static class OrderingGroupedAccumulator
            implements GroupedAccumulator
    {
        private final GroupedAccumulator accumulator;
        private final List<Integer> orderByChannels;
        private final List<SortOrder> orderings;
        private final PagesIndex pagesIndex;
        private long groupCount;

        private OrderingGroupedAccumulator(
                GroupedAccumulator accumulator,
                List<Type> aggregationSourceTypes,
                List<Integer> orderByChannels,
                List<SortOrder> orderings,
                PagesIndex.Factory pagesIndexFactory)
        {
            this.accumulator = requireNonNull(accumulator, "accumulator is null");
            requireNonNull(aggregationSourceTypes, "aggregationSourceTypes is null");
            this.orderByChannels = ImmutableList.copyOf(requireNonNull(orderByChannels, "orderByChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
            List<Type> pageIndexTypes = new ArrayList<>(aggregationSourceTypes);
            // Add group id column
            pageIndexTypes.add(BIGINT);
            this.pagesIndex = pagesIndexFactory.newPagesIndex(pageIndexTypes, 10_000);
            this.groupCount = 0;
        }

        @Override
        public long getEstimatedSize()
        {
            return pagesIndex.getEstimatedSize().toBytes() + accumulator.getEstimatedSize();
        }

        @Override
        public Type getFinalType()
        {
            return accumulator.getFinalType();
        }

        @Override
        public Type getIntermediateType()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addInput(ConnectorSession session, GroupByIdBlock groupIdsBlock, Page page)
        {
            Block[] blocks = new Block[page.getChannelCount() + 1];
            for (int i = 0; i < page.getChannelCount(); i++) {
                blocks[i] = page.getBlock(i);
            }
            // Add group id block
            blocks[page.getChannelCount()] = groupIdsBlock;
            groupCount = max(groupCount, groupIdsBlock.getGroupCount());
            pagesIndex.addPage(new Page(blocks));
        }

        @Override
        public void addIntermediate(GroupByIdBlock groupIdsBlock, Block block)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateIntermediate(int groupId, BlockBuilder output)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void evaluateFinal(int groupId, BlockBuilder output)
        {
            accumulator.evaluateFinal(groupId, output);
        }

        @Override
        public void prepareFinal()
        {
            pagesIndex.sort(orderByChannels, orderings);
            Iterator<Page> pagesIterator = pagesIndex.getSortedPages();
            pagesIterator.forEachRemaining(page -> {
                // The last channel of the page is the group id
                GroupByIdBlock groupIds = new GroupByIdBlock(groupCount, page.getBlock(page.getChannelCount() - 1));
                // We pass group id together with the other input channels to accumulator. Accumulator knows which input channels
                // to use. Since we did not change the order of original input channels, passing the group id is safe.

                // Super hack!!!
                accumulator.addInput(null, groupIds, page);
            });
        }
    }
}
