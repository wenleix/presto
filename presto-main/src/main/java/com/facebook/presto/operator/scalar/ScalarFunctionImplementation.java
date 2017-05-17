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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Boolean.FALSE;
import static java.util.Objects.requireNonNull;

public final class ScalarFunctionImplementation
{
    private final boolean nullable;
    private final List<Boolean> nullableArguments;
    private final List<Boolean> nullFlags;
    private final Map<Integer, Class> lambdaInterface;
    private final MethodHandle methodHandle;
    private final Optional<MethodHandle> instanceFactory;
    private final boolean deterministic;

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, MethodHandle methodHandle, boolean deterministic)
    {
        this(
                nullable,
                nullableArguments,
                Collections.nCopies(nullableArguments.size(), false),
                ImmutableMap.of(),
                methodHandle,
                Optional.empty(),
                deterministic);
    }

    public ScalarFunctionImplementation(boolean nullable, List<Boolean> nullableArguments, List<Boolean> nullFlags, MethodHandle methodHandle, boolean deterministic)
    {
        this(
                nullable,
                nullableArguments,
                nullFlags,
                ImmutableMap.of(),
                methodHandle,
                Optional.empty(),
                deterministic);
    }

    public ScalarFunctionImplementation(
            boolean nullable,
            List<Boolean> nullableArguments,
            List<Boolean> nullFlags,
            Map<Integer, Class> lambdaInterface,
            MethodHandle methodHandle,
            boolean deterministic)
    {
        this(
                nullable,
                nullableArguments,
                nullFlags,
                lambdaInterface,
                methodHandle,
                Optional.empty(),
                deterministic);
    }

    public ScalarFunctionImplementation(
            boolean nullable,
            List<Boolean> nullableArguments,
            List<Boolean> nullFlags,
            Map<Integer, Class> lambdaInterface,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory,
            boolean deterministic)
    {
        this.nullable = nullable;
        this.nullableArguments = ImmutableList.copyOf(requireNonNull(nullableArguments, "nullableArguments is null"));
        this.nullFlags = ImmutableList.copyOf(requireNonNull(nullFlags, "nullFlags is null"));
        this.lambdaInterface = ImmutableMap.copyOf(requireNonNull(lambdaInterface, "lambdaInterface is null"));
        this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");
        this.deterministic = deterministic;

        if (instanceFactory.isPresent()) {
            Class<?> instanceType = instanceFactory.get().type().returnType();
            checkArgument(instanceType.equals(methodHandle.type().parameterType(0)), "methodHandle is not an instance method");
        }
        // check lambda interface is not nullable
        for (int argumentIndex : lambdaInterface.keySet()) {
            checkArgument(FALSE.equals(nullableArguments.get(argumentIndex)), "argument %s marked as lambda is not nullable in method: %s", argumentIndex, methodHandle);
        }

        // check if nullableArguments and nullFlags match
        for (int i = 0; i < nullFlags.size(); i++) {
            if (nullFlags.get(i)) {
                checkArgument((boolean) nullableArguments.get(i), "argument %s marked as @IsNull is not nullable in method: %s", i, methodHandle);
            }
        }
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<Boolean> getNullableArguments()
    {
        return nullableArguments;
    }

    public List<Boolean> getNullFlags()
    {
        return nullFlags;
    }

    public Map<Integer, Class> getLambdaInterface()
    {
        return lambdaInterface;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    public Optional<MethodHandle> getInstanceFactory()
    {
        return instanceFactory;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }
}
