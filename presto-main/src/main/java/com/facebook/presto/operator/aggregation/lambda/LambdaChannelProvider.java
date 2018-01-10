package com.facebook.presto.operator.aggregation.lambda;

import com.facebook.presto.spi.ConnectorSession;

public interface LambdaChannelProvider
{
    // To support capture, need to enrich the interface into
    // getLambda(ConnectorSession session, Object[] capturedValues)
    Object getLambda(ConnectorSession session);
}
