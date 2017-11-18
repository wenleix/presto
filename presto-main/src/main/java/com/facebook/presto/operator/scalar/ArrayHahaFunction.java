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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.Math.toIntExact;

public class ArrayHahaFunction
        extends SqlScalarFunction
{
    public static final ArrayHahaFunction ARRAY_HAHA_FUNCTION = new ArrayHahaFunction();
    private static final String FUNCTION_NAME = "haha";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayHahaFunction.class, FUNCTION_NAME, Type.class, Type.class, BlockBuilder.class, Block.class);

    private ArrayHahaFunction()
    {
        super(new Signature(FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("E")),
                ImmutableList.of(),
                parseTypeSignature("array(E)"),
                ImmutableList.of(parseTypeSignature("array(E)")),
                false));
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Hahas the given array";
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = boundVariables.getTypeVariable("E");
        Type arrayType = typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(elementType.getTypeSignature())));
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(elementType).bindTo(arrayType);
        return new ScalarFunctionImplementation(
                false,
                ImmutableList.of(false),
                ImmutableList.of(false),
                ImmutableList.of(Optional.empty()),
                methodHandle,
                Optional.empty(),
                true,
                isDeterministic());
    }

    public static void haha(Type type, Type arrayType, BlockBuilder outputBlock, Block array)
    {
        BlockBuilder entryBlockBuilder = outputBlock.beginBlockEntry();
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (array.isNull(i)) {
                entryBlockBuilder.appendNull();
            }
            else {
                type.appendTo(array, i, entryBlockBuilder);
            }
        }
        outputBlock.closeEntry();
    }
}
