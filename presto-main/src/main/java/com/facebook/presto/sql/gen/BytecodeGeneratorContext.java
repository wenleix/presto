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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.gen.BytecodeUtils.generateInvocation;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class BytecodeGeneratorContext
{
    private final RowExpressionCompiler rowExpressionCompiler;
    private final Scope scope;
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final InvocationAdapter invocationAdapter;
    private final FunctionRegistry registry;
    private final PreGeneratedExpressions preGeneratedExpressions;
    private final Variable wasNull;

    public BytecodeGeneratorContext(
            RowExpressionCompiler rowExpressionCompiler,
            Scope scope,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            InvocationAdapter invocationAdapter,
            FunctionRegistry registry,
            PreGeneratedExpressions preGeneratedExpressions)
    {
        requireNonNull(rowExpressionCompiler, "bytecodeGenerator is null");
        requireNonNull(cachedInstanceBinder, "cachedInstanceBinder is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(callSiteBinder, "callSiteBinder is null");
        requireNonNull(registry, "registry is null");

        this.rowExpressionCompiler = rowExpressionCompiler;
        this.scope = scope;
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.invocationAdapter = invocationAdapter;
        this.registry = registry;
        this.preGeneratedExpressions = preGeneratedExpressions;
        this.wasNull = scope.getVariable("wasNull");
    }

    public Scope getScope()
    {
        return scope;
    }

    public CallSiteBinder getCallSiteBinder()
    {
        return callSiteBinder;
    }

    public BytecodeNode generate(RowExpression expression, Optional<Variable> outputBlock)
    {
        return generate(expression, outputBlock, Optional.empty());
    }

    public BytecodeNode generate(RowExpression expression, Optional<Variable> outputBlock, Optional<Class> lambdaInterface)
    {
        return rowExpressionCompiler.compile(expression, scope, outputBlock, lambdaInterface);
    }

    public FunctionRegistry getRegistry()
    {
        return registry;
    }

    public BytecodeNode generateCall(String name, ScalarFunctionImplementation function, List<BytecodeNode> arguments)
    {
        return generateCall(name, function, arguments, Optional.empty(), Optional.empty());
    }

    /**
     * Generates a function call with null handling, automatic binding of session parameter, etc.
     */
    public BytecodeNode generateCall(
            String name,
            ScalarFunctionImplementation function,
            List<BytecodeNode> arguments,
            Optional<Variable> outputBlock,
            Optional<Type> outputType)
    {
        if (outputBlock.isPresent() || function.isWriteToOutputBlock()) {
            checkArgument(outputType.isPresent(), "outputType must present if outputBlock is present, or function is writing to output block");
        }

        Optional<BytecodeNode> instance = Optional.empty();
        if (function.getInstanceFactory().isPresent()) {
            FieldDefinition field = cachedInstanceBinder.getCachedInstance(function.getInstanceFactory().get());
            instance = Optional.of(scope.getThis().getField(field));
        }
        return generateInvocation(callSiteBinder, invocationAdapter, scope, name, function, instance, arguments, outputBlock, outputType);
    }

    public Variable wasNull()
    {
        return wasNull;
    }
}
