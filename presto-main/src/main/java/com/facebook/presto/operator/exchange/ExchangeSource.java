package com.facebook.presto.operator.exchange;

import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.metadata.Split;
import com.google.common.util.concurrent.ListenableFuture;

public interface ExchangeSource
{
    void addSplit(Split split);

    void noMoreSplits();

    boolean isFinished();

    ListenableFuture<?> isBlocked();

    SerializedPage pollPage();

    void close();
}
