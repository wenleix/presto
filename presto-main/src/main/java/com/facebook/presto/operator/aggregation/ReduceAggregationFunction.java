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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationState;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationStateFactory;
import com.facebook.presto.operator.aggregation.state.ReduceAggregationStateSerializer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class ReduceAggregationFunction
        extends SqlAggregationFunction
{
    public static final ReduceAggregationFunction REDUCE_AGG = new ReduceAggregationFunction();
    private static final String NAME = "reduce_agg";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", Type.class, Type.class, ReduceAggregationState.class, Block.class, int.class, Object.class, MethodHandle.class, MethodHandle.class, MethodHandle.class);
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", Type.class, ReduceAggregationState.class, ReduceAggregationState.class, Object.class, MethodHandle.class, MethodHandle.class, MethodHandle.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "output", Type.class, Type.class, ReduceAggregationState.class, BlockBuilder.class, Object.class, MethodHandle.class, MethodHandle.class, MethodHandle.class);

    public ReduceAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T"), typeVariable("S"), typeVariable("R")),
                ImmutableList.of(),
                parseTypeSignature("R"),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("S"), parseTypeSignature("function(S,T,S)"), parseTypeSignature("function(S,T,S)"), parseTypeSignature("function(S,R)")));
    }

    @Override
    public String getDescription()
    {
        return "placeholder, too tired to think about it now, zzz...";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type inputType = boundVariables.getTypeVariable("T");
        Type stateType = boundVariables.getTypeVariable("S");
        Type outputType = boundVariables.getTypeVariable("R");
        return generateAggregation(inputType, stateType, outputType);
    }

    private InternalAggregationFunction generateAggregation(Type inputType, Type stateType, Type outputType)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ReduceAggregationFunction.class.getClassLoader());

        ReduceAggregationStateSerializer stateSerializer = new ReduceAggregationStateSerializer(stateType);

        List<Type> inputTypes = ImmutableList.of(inputType);

        ReduceAggregationStateFactory stateFactory = new ReduceAggregationStateFactory();
        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(getSignature().getName(), outputType.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(inputType),
                INPUT_FUNCTION.bindTo(inputType).bindTo(inputType).bindTo(stateType),
                COMBINE_FUNCTION.bindTo(stateType),
                OUTPUT_FUNCTION.bindTo(stateType).bindTo(outputType),
                ReduceAggregationState.class,
                stateSerializer,
                stateFactory,
                outputType);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getName(), inputTypes, stateType, outputType, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX));
    }

    public static void input(Type inputType, Type stateType, ReduceAggregationState state, Block block, int position, Object initialStateValue, MethodHandle mergeValue, MethodHandle mergeState, MethodHandle stateToOutput)
    {
        // Prototype only!!!
        Object currentStateValue = null;
        if (state.getState() == null) {
            currentStateValue = initialStateValue;
        }
        else {
            currentStateValue = readNativeValue(stateType, state.getState(), 0);
        }
        Object inputValue = readNativeValue(inputType, block, position);

        Object newStateValue;
        try {
            newStateValue = mergeValue.invoke(inputValue, currentStateValue);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }

        BlockBuilder newState = stateType.createBlockBuilder(new BlockBuilderStatus(), 1);
        writeNativeValue(stateType, newState, newStateValue);

        state.setState(newState);
    }

    public static void combine(Type stateType, ReduceAggregationState state, ReduceAggregationState otherState, Object initialStateValue, MethodHandle mergeValue, MethodHandle mergeState, MethodHandle stateToOutput)
    {
        Block stateBlock = state.getState();
        Block otherStateBlock = otherState.getState();

        if (otherStateBlock == null) {
            return;
        }
        else if (stateBlock == null && otherStateBlock != null) {
            state.setState(otherStateBlock);
            return;
        }
        else {
            Object stateValue = readNativeValue(stateType, stateBlock, 0);
            Object otherStateValue = readNativeValue(stateType, otherStateBlock, 0);

            Object newStateValue;
            try {
                newStateValue = mergeState.invoke(stateValue, otherStateValue);
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }

            // Prototype only!!!
            BlockBuilder newState = stateType.createBlockBuilder(new BlockBuilderStatus(), 1);
            writeNativeValue(stateType, newState, newStateValue);

            state.setState(newState);
        }
    }

    public static void output(Type stateType, Type outputType, ReduceAggregationState state, BlockBuilder out, Object initialStateValue, MethodHandle mergeValue, MethodHandle mergeState, MethodHandle stateToOutput)
    {
        Object stateValue = null;
        if (state.getState() != null) {
            stateValue = readNativeValue(stateType, state.getState(), 0);
        }

        Object outputValue = null;
        try {
            outputValue = stateToOutput.invoke(stateValue);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }

        writeNativeValue(outputType, out, outputValue);
    }
}
