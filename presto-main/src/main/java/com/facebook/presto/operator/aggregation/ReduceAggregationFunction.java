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
import com.facebook.presto.sql.gen.lambda.BinaryFunctionInterface;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.LAMBDA;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;

// Let's take one step back, it's actually an hidden merge_agg function :)
public class ReduceAggregationFunction
        extends SqlAggregationFunction
{
    public static final ReduceAggregationFunction REDUCE_AGG = new ReduceAggregationFunction();
    private static final String NAME = "reduce_agg";

    private static final MethodHandle INPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "input", Type.class, ReduceAggregationState.class, Block.class, int.class, BinaryFunctionInterface.class);

    private static final MethodHandle COMBINE_FUNCTION = methodHandle(ReduceAggregationFunction.class, "combine", Type.class, ReduceAggregationState.class, ReduceAggregationState.class);

    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(ReduceAggregationFunction.class, "output", Type.class, ReduceAggregationState.class, BlockBuilder.class);

    public ReduceAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("T"),
                ImmutableList.of(parseTypeSignature("T"), parseTypeSignature("function(T,T,T)")));
    }

    @Override
    public String getDescription()
    {
        return "placeholder, too tired to think about it now, zzz...";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type);
    }

    private InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(ReduceAggregationFunction.class.getClassLoader());

        ReduceAggregationStateSerializer stateSerializer = new ReduceAggregationStateSerializer(type);
        ReduceAggregationStateFactory stateFactory = new ReduceAggregationStateFactory();

        List<Type> inputTypes = ImmutableList.of(type);

        MethodHandle inputMethodHandle = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineMethodHandle = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputMethodHandle = OUTPUT_FUNCTION.bindTo(type);

        AggregationMetadata metadata = new AggregationMetadata(
                // TODO: this name is not correct, need to find someway to generate name based on hash of lambda
                generateAggregationName(getSignature().getName(), type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                createInputParameterMetadata(type),
                inputMethodHandle,
                combineMethodHandle,
                outputMethodHandle,
                ReduceAggregationState.class,
                stateSerializer,
                stateFactory,
                type);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(getSignature().getName(), inputTypes, type, type, true, false, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type type)
    {
        return ImmutableList.of(
                new ParameterMetadata(STATE),
                new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, type),
                new ParameterMetadata(BLOCK_INDEX),

                // TODO: Add functional interface into the ParameterMetadata
                new ParameterMetadata(LAMBDA)
        );
    }

    public static void input(Type type, ReduceAggregationState state, Block block, int position, BinaryFunctionInterface mergeValue)
    {
        // Prototype only!!!
        Object currentStateValue = null;
        Object newStateValue;

        if (state.getState() == null) {
            newStateValue = readNativeValue(type, block, position);
        }
        else {
            currentStateValue = readNativeValue(type, state.getState(), 0);
            Object inputValue = readNativeValue(type, block, position);

            try {
                newStateValue = mergeValue.apply(currentStateValue, inputValue);
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }

        // It's a joke...
        BlockBuilder newState = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        writeNativeValue(type, newState, newStateValue);

        state.setState(newState);
    }

    public static void combine(Type stateType, ReduceAggregationState state, ReduceAggregationState otherState)
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
            // Hard coded :D
            Long stateValue = (Long) readNativeValue(stateType, stateBlock, 0);
            Long otherStateValue = (Long) readNativeValue(stateType, otherStateBlock, 0);
            Long newStateValue = stateValue * otherStateValue;

            // It's a joke...
            BlockBuilder newState = stateType.createBlockBuilder(new BlockBuilderStatus(), 1);
            writeNativeValue(stateType, newState, newStateValue);

            state.setState(newState);
        }
    }

    public static void output(Type type, ReduceAggregationState state, BlockBuilder out)
    {
        Object stateValue = null;
        if (state.getState() != null) {
            stateValue = readNativeValue(type, state.getState(), 0);
        }
        writeNativeValue(type, out, stateValue);
    }
}
