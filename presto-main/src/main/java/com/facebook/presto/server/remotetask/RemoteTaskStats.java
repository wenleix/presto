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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

public class RemoteTaskStats
{
    private final IncrementalAverage updateRoundTripMillis = new IncrementalAverage();
    private final IncrementalAverage infoRoundTripMillis = new IncrementalAverage();
    private final IncrementalAverage statusRoundTripMillis = new IncrementalAverage();
    private final IncrementalAverage responseSizeBytes = new IncrementalAverage();
    private final DistributionStat updateWithPlanBytes = new DistributionStat();

    private final IncrementalSum sessionSerializationNanos = new IncrementalSum();
    private final IncrementalSum extraCredentialNanos = new IncrementalSum();
    private final IncrementalSum sourcesNanos = new IncrementalSum();
    private final IncrementalSum outputIdsNanos = new IncrementalSum();
    private final IncrementalSum planSerializationNanos = new IncrementalSum();

    private long requestSuccess;
    private long requestFailure;

    public void sessionSerializationNanos(long nanos)
    {
        sessionSerializationNanos.add(nanos);
    }

    public void extraCredentialNanos(long nanos)
    {
        extraCredentialNanos.add(nanos);
    }

    public void sourceNanos(long nanos)
    {
        sourcesNanos.add(nanos);
    }

    public void outputIdsNanos(long nanos)
    {
        outputIdsNanos.add(nanos);
    }

    public void planSerializationNanos(long nanos)
    {
        planSerializationNanos.add(nanos);
    }

    public void statusRoundTripMillis(long roundTripMillis)
    {
        statusRoundTripMillis.add(roundTripMillis);
    }

    public void infoRoundTripMillis(long roundTripMillis)
    {
        infoRoundTripMillis.add(roundTripMillis);
    }

    public void updateRoundTripMillis(long roundTripMillis)
    {
        updateRoundTripMillis.add(roundTripMillis);
    }

    public void responseSize(long responseSizeBytes)
    {
        this.responseSizeBytes.add(responseSizeBytes);
    }

    public void updateSuccess()
    {
        requestSuccess++;
    }

    public void updateFailure()
    {
        requestFailure++;
    }

    public void updateWithPlanBytes(long bytes)
    {
        updateWithPlanBytes.add(bytes);
    }

    @Managed
    public double getSessionSerializationNanos()
    {
        return sessionSerializationNanos.getSum();
    }

    @Managed
    public double getExtraCredentialNanos()
    {
        return extraCredentialNanos.getSum();
    }

    @Managed
    public double getSourcesNanos()
    {
        return sourcesNanos.getSum();
    }

    @Managed
    public double getOutputIdsNanos()
    {
        return outputIdsNanos.getSum();
    }

    @Managed
    public double getPlanSerializationNanos()
    {
        return planSerializationNanos.getSum();
    }

    @Managed
    public double getTaskUpdateRequestCount()
    {
        return sessionSerializationNanos.getCount();
    }

    @Managed
    public double getPlanSerializationCount()
    {
        return planSerializationNanos.getCount();
    }

    @Managed
    public double getResponseSizeBytes()
    {
        return responseSizeBytes.getAverage();
    }

    @Managed
    public double getStatusRoundTripMillis()
    {
        return statusRoundTripMillis.getAverage();
    }

    @Managed
    public long getStatusRoundTripCount()
    {
        return statusRoundTripMillis.getCount();
    }

    @Managed
    public double getUpdateRoundTripMillis()
    {
        return updateRoundTripMillis.getAverage();
    }

    @Managed
    public long getUpdateRoundTripCount()
    {
        return updateRoundTripMillis.getCount();
    }

    @Managed
    public double getInfoRoundTripMillis()
    {
        return infoRoundTripMillis.getAverage();
    }

    @Managed
    public long getInfoRoundTripCount()
    {
        return infoRoundTripMillis.getCount();
    }

    @Managed
    public long getRequestSuccess()
    {
        return requestSuccess;
    }

    @Managed
    public long getRequestFailure()
    {
        return requestFailure;
    }

    @Managed
    @Nested
    public DistributionStat getUpdateWithPlanBytes()
    {
        return updateWithPlanBytes;
    }

    @ThreadSafe
    private static class IncrementalAverage
    {
        private volatile long count;
        private volatile double average;

        synchronized void add(long value)
        {
            count++;
            average = average + (value - average) / count;
        }

        double getAverage()
        {
            return average;
        }

        long getCount()
        {
            return count;
        }
    }

    @ThreadSafe
    private static class IncrementalSum
    {
        private volatile long count;
        private volatile long sum;

        synchronized void add(long value)
        {
            count++;
            sum += value;
        }

        long getSum()
        {
            return sum;
        }

        long getCount()
        {
            return count;
        }
    }
}
