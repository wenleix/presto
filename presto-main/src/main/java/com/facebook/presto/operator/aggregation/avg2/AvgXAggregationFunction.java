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
package com.facebook.presto.operator.aggregation.avg2;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.AggregationMetadata;
import com.facebook.presto.operator.aggregation.GenericAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.state.LongAndDoubleState;
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
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public class AvgXAggregationFunction
        extends SqlAggregationFunction
{
    public static final AvgXAggregationFunction AVERAGEX_AGGREGATION = new AvgXAggregationFunction();
    private static final String NAME = "avgx";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(AvgXAggregationFunction.class, "input", LongAndDoubleState.class, double.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(AvgXAggregationFunction.class, "combine", LongAndDoubleState.class, LongAndDoubleState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(AvgXAggregationFunction.class, "output", LongAndDoubleState.class, BlockBuilder.class);

    protected AvgXAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.DOUBLE),
                ImmutableList.of(parseTypeSignature(StandardTypes.DOUBLE)));
    }

    @Override
    public String getDescription()
    {
        return "average X";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(AvgXAggregationFunction.class.getClassLoader());
        Class<? extends AccumulatorState> stateInterface = LongAndDoubleState.class;
        AccumulatorStateSerializer<?> stateSerializer = StateCompiler.generateStateSerializer(stateInterface, classLoader);

        Type intermediateType = stateSerializer.getSerializedType();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, parseTypeSignature(StandardTypes.DOUBLE), ImmutableList.of(parseTypeSignature(StandardTypes.DOUBLE))),
                ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(INPUT_CHANNEL, DOUBLE)),
                INPUT_FUNCTION,
                COMBINE_FUNCTION,
                OUTPUT_FUNCTION,
                stateInterface,
                stateSerializer,
                StateCompiler.generateStateFactory(stateInterface, classLoader),
                DOUBLE);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(
                NAME,
                ImmutableList.of(DOUBLE),
                stateSerializer.getSerializedType(),
                DOUBLE,
                true,
                false,
                factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(LongAndDoubleState state, double value)
    {
        state.setLong(state.getLong() + 1);
        state.setDouble(state.getDouble() + value);
    }

    public static void combine(LongAndDoubleState state, LongAndDoubleState otherState)
    {
        state.setLong(state.getLong() + otherState.getLong());
        state.setDouble(state.getDouble() + otherState.getDouble());
    }

    public static void output(LongAndDoubleState state, BlockBuilder out)
    {
        long count = state.getLong();
        if (count == 0) {
            out.appendNull();
        }
        else {
            double value = state.getDouble();
            DOUBLE.writeDouble(out, value / count);
        }
    }
}
