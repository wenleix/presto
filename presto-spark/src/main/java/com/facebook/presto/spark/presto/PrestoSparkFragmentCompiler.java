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
package com.facebook.presto.spark.presto;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.buffer.PagesSerdeUtil;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spark.spi.PrestoConfiguration;
import com.facebook.presto.spark.spi.SparkFragmentCompiler;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import scala.Tuple2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;

import static com.facebook.presto.execution.buffer.PagesSerdeUtil.readSerializedPages;
import static com.facebook.presto.spark.presto.LocalQueryRunnerFactory.createJsonCodec;
import static com.facebook.presto.spark.presto.LocalQueryRunnerFactory.createLocalQueryRunner;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PrestoSparkFragmentCompiler
        implements SparkFragmentCompiler
{
    private final PrestoConfiguration prestoConfiguration;

    public PrestoSparkFragmentCompiler(PrestoConfiguration prestoConfiguration)
    {
        this.prestoConfiguration = requireNonNull(prestoConfiguration, "prestoConfiguration is null");
    }

    @Override
    public Iterator<Tuple2<Integer, byte[]>> compileFragment(String fragmentJson, Map<String, Iterator<Tuple2<Integer, byte[]>>> inputs)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner(prestoConfiguration);
        SparkTaskRequest request = createJsonCodec(SparkTaskRequest.class, localQueryRunner.getHandleResolver()).fromJson(fragmentJson);
        System.out.println(request.getFragment());
        System.out.println(request.getSources());

        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(2, GIGABYTE));
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(
                new QueryId("test"),
                new DataSize(1, GIGABYTE),
                new DataSize(2, GIGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                localQueryRunner.getExecutor(),
                localQueryRunner.getScheduler(),
                new DataSize(4, GIGABYTE),
                spillSpaceTracker);

        TaskContext taskContext = queryContext
                .addTaskContext(new TaskStateMachine(new TaskId("query", 0, 0, 0), localQueryRunner.getExecutor()),
                        localQueryRunner.getDefaultSession(),
                        false,
                        false,
                        OptionalInt.empty(),
                        false);

        SparkOutputBuffer outputBuffer = new SparkOutputBuffer();
        LocalExecutionPlan localExecutionPlan = localQueryRunner.createLocalExecutionPlan(
                outputBuffer,
                taskContext,
                request.getFragment(),
                inputs.entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> new PlanNodeId(entry.getKey()),
                                entry -> Iterators.transform(entry.getValue(), tupple -> deserializeSerializedPage(tupple._2)))));

        List<Driver> drivers = localQueryRunner.createDrivers(
                localExecutionPlan,
                taskContext,
                request.getFragment(),
                request.getSources());

        String description = format("fragment: %s, processor: %s", request.getFragment().getId(), UUID.randomUUID().toString());
        return new SparkDriverProcessor(description, drivers, outputBuffer);
    }

    private static class SparkDriverProcessor
            extends AbstractIterator<Tuple2<Integer, byte[]>>
    {
        private final String description;
        private final List<Driver> drivers;
        private final SparkOutputBuffer outputBuffer;

        private SparkDriverProcessor(String description, List<Driver> drivers, SparkOutputBuffer outputBuffer)
        {
            this.description = description;
            this.drivers = drivers;
            this.outputBuffer = outputBuffer;
        }

        @Override
        protected Tuple2<Integer, byte[]> computeNext()
        {
            boolean done = false;
            while (!done && !outputBuffer.hasPagesBuffered()) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (!driver.isFinished()) {
                        driver.process();
                        processed = true;
                    }
                }
                done = !processed;
            }

            if (done && !outputBuffer.hasPagesBuffered()) {
                return endOfData();
            }

            PageWithPartition pageWithPartition = outputBuffer.getNext();
            SerializedPage serializedPage = pageWithPartition.getPage();
            System.out.printf("Producing page for partition: %s (%s)\n", pageWithPartition.getPartition(), description);
            return new Tuple2<>(pageWithPartition.getPartition(), serializePage(serializedPage));
        }
    }

    private static byte[] serializePage(SerializedPage page)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput(page.getUncompressedSizeInBytes());
        PagesSerdeUtil.writeSerializedPage(sliceOutput, page);
        try {
            sliceOutput.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return sliceOutput.getUnderlyingSlice().getBytes();
    }

    private static SerializedPage deserializeSerializedPage(byte[] data)
    {
        return readSerializedPages(Slices.wrappedBuffer(data).getInput()).next();
    }
}
