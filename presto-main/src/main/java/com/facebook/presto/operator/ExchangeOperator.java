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
package com.facebook.presto.operator;

import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.exchange.ExchangeSource;
import com.facebook.presto.operator.exchange.SparkExchangeSource;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ExchangeOperator
        implements SourceOperator, Closeable
{
    public static final ConnectorId REMOTE_CONNECTOR_ID = new ConnectorId("$remote");

    public static class ExchangeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final TaskExchangeClientManager taskExchangeClientManager;
        private final PagesSerdeFactory serdeFactory;
        private ExchangeSource exchangeSource;
        private final Optional<SparkExchangeSource> sparkExchangeSource;

        private boolean closed;

        public ExchangeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                TaskExchangeClientManager taskExchangeClientManager,
                PagesSerdeFactory serdeFactory,
                // !@#$%^&*
                Optional<SparkExchangeSource> sparkExchangeSource)
        {
            this.operatorId = operatorId;
            this.sourceId = sourceId;
            this.taskExchangeClientManager = requireNonNull(taskExchangeClientManager, "taskExchangeClientManager is null");
            this.sparkExchangeSource = requireNonNull(sparkExchangeSource, "sparkExchangeSource is null");
            this.serdeFactory = serdeFactory;

            if (sparkExchangeSource.isPresent()) {
                taskExchangeClientManager = null;
                exchangeSource = sparkExchangeSource.get();
            }
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, ExchangeOperator.class.getSimpleName());

            if (exchangeSource == null) {
                exchangeSource = taskExchangeClientManager.createExchangeClient(driverContext.getPipelineContext().localSystemMemoryContext());
            }

            return new ExchangeOperator(
                    operatorContext,
                    sourceId,
                    serdeFactory.createPagesSerde(),
                    exchangeSource);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeSource exchangeSource;
    private final PagesSerde serde;

    public ExchangeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            PagesSerde serde,
            ExchangeSource exchangeSource)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeSource = requireNonNull(exchangeSource, "exchangeSource is null");
        this.serde = requireNonNull(serde, "serde is null");

        // !@#$%^&&
        operatorContext.setInfoSupplier(() -> new ExchangeClientStatus(0, 0, 0, 0, 0, false, ImmutableList.of()));
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        exchangeSource.addSplit(split);

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        exchangeSource.noMoreSplits();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return exchangeSource.isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = exchangeSource.isBlocked();
        if (blocked.isDone()) {
            return NOT_BLOCKED;
        }
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        SerializedPage page = exchangeSource.pollPage();
        if (page == null) {
            return null;
        }

        operatorContext.recordRawInput(page.getSizeInBytes(), page.getPositionCount());

        Page deserializedPage = serde.deserialize(page);
        operatorContext.recordProcessedInput(deserializedPage.getSizeInBytes(), page.getPositionCount());

        return deserializedPage;
    }

    @Override
    public void close()
    {
        exchangeSource.close();
    }
}
