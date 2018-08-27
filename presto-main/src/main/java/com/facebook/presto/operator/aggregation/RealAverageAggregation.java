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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.StateCompiler;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.DynamicClassLoader;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;

public class RealAverageAggregation
        extends SqlAggregationFunction
{
    public static final RealAverageAggregation REAL_AVERAGE_AGGREGATION = new RealAverageAggregation();
    private static final String NAME = "avg";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(RealAverageAggregation.class, "input", NullableLongState.class, NullableDoubleState.class, long.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(RealAverageAggregation.class, "combine", NullableLongState.class, NullableDoubleState.class, NullableLongState.class, NullableDoubleState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(RealAverageAggregation.class, "output", NullableLongState.class, NullableDoubleState.class, BlockBuilder.class);

    protected RealAverageAggregation()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.REAL),
                ImmutableList.of(parseTypeSignature(StandardTypes.REAL)));
    }

    @Override
    public String getDescription()
    {
        return "Returns the average value of the argument";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AverageAggregations.class.getClassLoader());
        Class<? extends AccumulatorState> longStateInterface = NullableLongState.class;
        Class<? extends AccumulatorState> doubleStateInterface = NullableDoubleState.class;
        AccumulatorStateSerializer<?> longStateSerializer = StateCompiler.generateStateSerializer(longStateInterface, classLoader);
        AccumulatorStateSerializer<?> doubleStateSerializer = StateCompiler.generateStateSerializer(doubleStateInterface, classLoader);

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, parseTypeSignature(StandardTypes.REAL), ImmutableList.of(parseTypeSignature(StandardTypes.REAL))),
                ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(STATE), new ParameterMetadata(INPUT_CHANNEL, REAL)),
                INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                ImmutableList.of(longStateInterface, doubleStateInterface),
                ImmutableList.of(longStateSerializer, doubleStateSerializer),
                ImmutableList.of(
                        StateCompiler.generateStateFactory(longStateInterface, classLoader),
                        StateCompiler.generateStateFactory(doubleStateInterface, classLoader)),
                REAL);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(
                NAME,
                ImmutableList.of(REAL),
                ImmutableList.of(
                        longStateSerializer.getSerializedType(),
                        doubleStateSerializer.getSerializedType()),
                REAL,
                true,
                false,
                factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(NullableLongState count, NullableDoubleState sum, long value)
    {
        if (count.isNull()) {
            count.setNull(false);
            count.setLong(1);
            sum.setNull(false);
            sum.setDouble(intBitsToFloat((int) value));
            return;
        }

        count.setLong(count.getLong() + 1);
        sum.setDouble(sum.getDouble() + intBitsToFloat((int) value));
    }

    public static void combine(NullableLongState count, NullableDoubleState sum, NullableLongState otherCount, NullableDoubleState otherSum)
    {
        if (count.isNull()) {
            count.setNull(false);
            count.setLong(otherCount.getLong());
            sum.setDouble(otherSum.getDouble());
            return;
        }

        count.setLong(count.getLong() + otherCount.getLong());
        sum.setDouble(sum.getDouble() + otherSum.getDouble());
    }

    public static void output(NullableLongState count, NullableDoubleState sum, BlockBuilder out)
    {
        if (count.isNull() || count.getLong() == 0) {
            out.appendNull();
        }
        else {
            REAL.writeLong(out, floatToIntBits((float) (sum.getDouble() / count.getLong())));
        }
    }
}