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

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpression;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantClass;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.setStatic;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.boxPrimitiveIfNecessary;
import static com.facebook.presto.sql.gen.BytecodeUtils.unboxPrimitiveIfNecessary;
import static com.facebook.presto.sql.relational.Signatures.BIND;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LambdaBytecodeGenerator
{
    private LambdaBytecodeGenerator()
    {
    }

    /**
     * @return a MethodHandle field that represents the lambda expression
     */
    public static LambdaExpressionField preGenerateLambdaExpression(
            LambdaDefinitionExpression lambdaExpression,
            String fieldName,
            ClassDefinition classDefinition,
            PreGeneratedExpressions preGeneratedExpressions,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            FunctionRegistry functionRegistry)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        ImmutableMap.Builder<String, ParameterAndType> parameterMapBuilder = ImmutableMap.builder();

        parameters.add(arg("session", ConnectorSession.class));
        for (int i = 0; i < lambdaExpression.getArguments().size(); i++) {
            Class<?> type = Primitives.wrap(lambdaExpression.getArgumentTypes().get(i).getJavaType());
            String argumentName = lambdaExpression.getArguments().get(i);
            Parameter arg = arg("lambda_" + argumentName, type);
            parameters.add(arg);
            parameterMapBuilder.put(argumentName, new ParameterAndType(arg, type));
        }

        BytecodeExpressionVisitor innerExpressionVisitor = new BytecodeExpressionVisitor(
                callSiteBinder,
                cachedInstanceBinder,
                variableReferenceCompiler(parameterMapBuilder.build()),
                functionRegistry,
                preGeneratedExpressions);

        return defineLambdaMethodAndField(
                innerExpressionVisitor,
                classDefinition,
                fieldName,
                parameters.build(),
                lambdaExpression);
    }

    private static LambdaExpressionField defineLambdaMethodAndField(
            BytecodeExpressionVisitor innerExpressionVisitor,
            ClassDefinition classDefinition,
            String fieldAndMethodName,
            List<Parameter> inputParameters,
            LambdaDefinitionExpression lambda)
    {
        Class<?> returnType = Primitives.wrap(lambda.getBody().getType().getJavaType());
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), fieldAndMethodName, type(returnType), inputParameters);

        Scope scope = method.getScope();
        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        BytecodeNode compiledBody = lambda.getBody().accept(innerExpressionVisitor, scope);
        method.getBody()
                .putVariable(wasNull, false)
                .append(compiledBody)
                .append(boxPrimitiveIfNecessary(scope, returnType))
                .ret(returnType);

        FieldDefinition staticField = classDefinition.declareField(a(PRIVATE, STATIC, FINAL), fieldAndMethodName, type(MethodHandle.class));
        FieldDefinition instanceField = classDefinition.declareField(a(PRIVATE, FINAL), "binded_" + fieldAndMethodName, type(MethodHandle.class));

        classDefinition.getClassInitializer().getBody()
                .append(setStatic(
                        staticField,
                        invokeStatic(
                                Reflection.class,
                                "methodHandle",
                                MethodHandle.class,
                                constantClass(classDefinition.getType()),
                                constantString(fieldAndMethodName),
                                newArray(
                                        type(Class[].class),
                                        inputParameters.stream()
                                                .map(Parameter::getType)
                                                .map(BytecodeExpressions::constantClass)
                                                .collect(toImmutableList())))));

        return new LambdaExpressionField(staticField, instanceField);
    }

    public static BytecodeNode generateLambda(BytecodeGeneratorContext context, RowExpression lambdaExpression, Map<LambdaDefinitionExpression, LambdaExpressionField> lambdaFieldsMap, Class lambdaInterface)
    {
        if (lambdaExpression instanceof CallExpression) {
            CallExpression bindCall = (CallExpression) lambdaExpression;
            checkCondition(bindCall.getSignature().getName() == BIND, COMPILER_ERROR, "Lambda expression should be the direct argument to a function call");

            return generateLambda(
                    context,
                    bindCall.getArguments(),
                    lambdaFieldsMap,
                    lambdaInterface);
        }
        else if (lambdaExpression instanceof LambdaDefinitionExpression) {
            return generateLambda(
                    context,
                    ImmutableList.of(lambdaExpression),
                    lambdaFieldsMap,
                    lambdaInterface);
        }
        else {
            checkCondition(false, COMPILER_ERROR, "Expect lambda expression");
            return null;
        }
    }

    public static BytecodeNode generateLambda(
            BytecodeGeneratorContext context,
            List<RowExpression> arguments,
            Map<LambdaDefinitionExpression, LambdaExpressionField> lambdaFieldsMap,
            Class lambdaInterface)
    {
        if (!MethodHandle.class.equals(lambdaInterface)) {
            throw new UnsupportedOperationException();
        }

        BytecodeBlock block = new BytecodeBlock().setDescription("Partial apply");
        Scope scope = context.getScope();

        Variable wasNull = scope.getVariable("wasNull");

        ImmutableList.Builder<BytecodeExpression> captureVariablesBuilder = ImmutableList.builder();
        int numValues = arguments.size() - 1;
        for (int i = 0; i < numValues; i++) {
            Class<?> valueType = Primitives.wrap(arguments.get(i).getType().getJavaType());
            Variable valueVariable = scope.createTempVariable(valueType);
            block.append(context.generate(arguments.get(i)));
            block.append(boxPrimitiveIfNecessary(scope, valueType));
            block.putVariable(valueVariable);
            block.append(wasNull.set(constantFalse()));
            captureVariablesBuilder.add(valueVariable.cast(Object.class));
        }

        Variable functionVariable = scope.createTempVariable(MethodHandle.class);
        block.append(context.generate(arguments.get(numValues)));
        block.append(
                new IfStatement()
                        .condition(wasNull)
                        // ifTrue: do nothing i.e. Leave the null MethodHandle on the stack, and leave the wasNull variable set to true
                        .ifFalse(
                                new BytecodeBlock()
                                        .putVariable(functionVariable)
                                        .append(invokeStatic(
                                                MethodHandles.class,
                                                "insertArguments",
                                                MethodHandle.class,
                                                functionVariable,
                                                constantInt(0),
                                                newArray(type(Object[].class), captureVariablesBuilder.build())))));

        return block;
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> variableReferenceCompiler(Map<String, ParameterAndType> parameterMap)
    {
        return new RowExpressionVisitor<BytecodeNode, Scope>()
        {
            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression node, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitCall(CallExpression call, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitConstant(ConstantExpression literal, Scope scope)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Scope context)
            {
                ParameterAndType parameterAndType = parameterMap.get(reference.getName());
                Parameter parameter = parameterAndType.getParameter();
                Class<?> type = parameterAndType.getType();
                return new BytecodeBlock()
                        .append(parameter)
                        .append(unboxPrimitiveIfNecessary(context, type));
            }
        };
    }

    static class LambdaExpressionField
    {
        private final FieldDefinition staticField;
        // the instance field will be binded to "this" in constructor
        private final FieldDefinition instanceField;

        public LambdaExpressionField(FieldDefinition staticField, FieldDefinition instanceField)
        {
            this.staticField = requireNonNull(staticField, "staticField is null");
            this.instanceField = requireNonNull(instanceField, "instanceField is null");
        }

        public FieldDefinition getInstanceField()
        {
            return instanceField;
        }

        public void generateInitialization(Variable thisVariable, BytecodeBlock block)
        {
            block.append(
                    thisVariable.setField(
                            instanceField,
                            getStatic(staticField).invoke("bindTo", MethodHandle.class, thisVariable.cast(Object.class))));
        }
    }
}
