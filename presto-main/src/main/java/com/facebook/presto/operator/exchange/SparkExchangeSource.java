package com.facebook.presto.operator.exchange;

import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Page;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;

import static com.facebook.presto.operator.Operator.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

// !@#$%^&*
public class SparkExchangeSource
    implements ExchangeSource
{
    private Iterator<SerializedPage> iterator;

    public SparkExchangeSource(Iterator<SerializedPage> iterator)
    {
        this.iterator = requireNonNull(iterator, "iterator is null");
    }

    @Override
    public void addSplit(Split split)
    {
        // TODO: Maybe we want to have some kind of SparkSplit? -- But we don't want to make it
        // to be in presto-main
        throw new UnsupportedOperationException();
    }

    @Override
    public void noMoreSplits()
    {
        // no-op
    }

    @Override
    public boolean isFinished()
    {
        return !iterator.hasNext();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public SerializedPage pollPage()
    {
        if (!iterator.hasNext()) {
            return null;
        }
        else {
            return iterator.next();
        }
    }

    @Override
    public void close()
    {
        iterator = null;
    }
}
