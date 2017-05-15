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
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.LambdaExpressionField;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantClass;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantLong;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public class BindCodeGenerator
        implements BytecodeGenerator
{
    private final Map<LambdaDefinitionExpression, LambdaExpressionField> lambdaFieldsMap;
    private final Class targetInterface;

    public BindCodeGenerator(Map<LambdaDefinitionExpression, LambdaExpressionField> lambdaFieldsMap, Class targetInterface)
    {
        this.lambdaFieldsMap = requireNonNull(lambdaFieldsMap, "lambdaFieldsMap is null");
        this.targetInterface = requireNonNull(targetInterface, "targetClass is null");
    }

    @Override
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments)
    {
        BytecodeBlock block = new BytecodeBlock().setDescription("Partial apply");
        Scope scope = context.getScope();

        Variable wasNull = scope.getVariable("wasNull");

        /*
        Class<?> valueType = Primitives.wrap(arguments.get(0).getType().getJavaType());
        Variable valueVariable = scope.createTempVariable(valueType);
        block.append(context.generate(arguments.get(0)));
        block.append(boxPrimitiveIfNecessary(scope, valueType));
        block.putVariable(valueVariable);
        block.append(wasNull.set(constantFalse()));
        */

        Variable functionVariable = scope.createTempVariable(MethodHandle.class);

        checkState(arguments.get(1) instanceof LambdaDefinitionExpression, "must be a lambda definition expression!");

        Variable lambdaFactory = scope.createTempVariable(MethodHandle.class);

//        block.append(context.generate(arguments.get(1)));

        /*
                MethodHandle factory = LambdaMetafactory.metafactory(
                MethodHandles.lookup(),
                "apply",
                MethodType.methodType(Function.class, LambdaMetaFactoryBenchmark_MoreInvoke.class, Long.class),    // arg1 -> CapturedLambda
                MethodType.methodType(Object.class, Object.class),              // arg2 -> ret, after type erasure
                methodHandle,                                                   // Original method, (arg1, arg2) -> ret
                MethodType.methodType(Long.class, Long.class)                   // arg2 -> ret, original type
        ).getTarget();
         */

        LambdaDefinitionExpression lambdaDefinitionExpression = (LambdaDefinitionExpression) arguments.get(1);
        Class capturedJavaType = lambdaDefinitionExpression.getArgumentTypes().get(0).getJavaType();
        List<Class> uncapturedJavaType = lambdaDefinitionExpression.getArgumentTypes().stream()
                .skip(1)
                .map(Type::getJavaType)
                .collect(Collectors.toList());
        Class returnJavaType = lambdaDefinitionExpression.getBody().getType().getJavaType();

        // Find the method to be implemented
        MethodType applyMethodType = getApplyMethodType(targetInterface);

        block.append(
                new BytecodeBlock()
                    .append(
                            invokeStatic(
                                LambdaMetafactory.class,
                                "metafactory",
                                CallSite.class,
                                invokeStatic(MethodHandles.class, "lookup", MethodHandles.Lookup.class),
                                constantString("apply"),
                                invokeStatic(
                                        MethodType.class,
                                        "methodType",
                                        MethodType.class,
                                        constantClass(targetInterface),
                                        newArray(type(Class[].class),
                                                ImmutableList.of(
                                                        constantClass(scope.getThis().getType()),
                                                        constantClass(ConnectorSession.class),
                                                        constantClass(Long.class)))),
                                invokeStatic(
                                        MethodType.class,
                                        "methodType",
                                        MethodType.class,
                                        constantClass(Object.class),
                                        newArray(type(Class[].class), ImmutableList.of(constantClass(Object.class)))),
                                getStatic(lambdaFieldsMap.get(arguments.get(1)).getStaticField()),  // original MethodHandle
                                invokeStatic(
                                        MethodType.class,
                                        "methodType",
                                        MethodType.class,
                                        constantClass(Long.class),
                                        newArray(type(Class[].class), ImmutableList.of(constantClass(Long.class))))

                                )
                                .invoke("getTarget", MethodHandle.class)
                                .invoke("invokeExact",
                                        targetInterface,
                                        scope.getThis(),
                                        scope.getVariable("session"),
                                        invokeStatic(Long.class, "valueOf", Long.class, constantLong(9)))
                    )
        );

        return block;
    }

    private MethodType getApplyMethodType(Class targetInterface)
    {
        List<Method> applyMethods = Arrays.stream(targetInterface.getMethods())
                .filter(method -> method.getName().equals("apply"))
                .collect(Collectors.toList());

       checkCondition(applyMethods.size() == 1, COMPILER_ERROR, "Expect to have exact method with name 'apply' in interface " + targetInterface.getName());
       Method applyMethod = applyMethods.get(0);
       return methodType(applyMethod.getReturnType(), applyMethod.getParameterTypes());
    }
}
