package com.facebook.presto.operator.aggregation.lambda;

import com.facebook.presto.spi.ConnectorSession;

// Lambda has to be compiled into a dedicated class, since functions might be stateful (e.g. use CachedInstanceBinder)
// Similar to PageProjectionWork
public interface LambdaChannelProvider
{
    // To support capture, we can enrich the interface into
    // getLambda(Object[] capturedValues)

    // The lambda capture is done through invokedynamic, and the CallSite will be cached after
    // the first call. Thus for different captures, different classes have to be generated.
    Object getLambda();
}
