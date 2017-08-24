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
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.DependentFunctionInfo;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import com.sun.tools.classfile.Annotation;
import io.airlift.slice.Slice;

import javax.ws.rs.NotSupportedException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.STATIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantClass;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantString;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.getStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newInstance;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.setStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.subtract;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.BytecodeUtils.loadConstant;
import static com.facebook.presto.sql.gen.SqlTypeBytecodeExpression.constantType;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class InvocationAdapter
{
    private final ClassDefinition classDefinition;
    private final CallSiteBinder callSiteBinder;
    private final Map<FieldDefinition, Type> pageBuilderFields = new HashMap<>();
    private final List<AdaptedScalarFunctionField> adaptedScalarFunctionFields = new ArrayList<>();

    private int nextId;

    private final static MethodHandle CREATE_PAGE_BUILDER_METHOD_HANDLE = methodHandle(InvocationAdapter.class, "createPageBuilder", Type.class);

    public InvocationAdapter(ClassDefinition classDefinition, CallSiteBinder callSiteBinder)
    {
        this.classDefinition = requireNonNull(classDefinition, "classDefinition is null");
        this.callSiteBinder = requireNonNull(callSiteBinder, "callSiteBinder is null");
    }

    public CallSiteBinder getCallSiteBinder()
    {
        return callSiteBinder;
    }


    // Adapt the return convention and bind the dependent functions
    public AdaptedScalarFunction getAdaptedScalarFunction(String name, ScalarFunctionImplementation implementation, boolean writeToOutputBlock, Optional<Type> outputType)
    {
        if (writeToOutputBlock || implementation.isWriteToOutputBlock()) {
            checkArgument(outputType.isPresent(), "outputType must present if outputBlock is present, or function is writing to output block");
        }

        String fieldAndMethodName = "adaptedScalarFunction_" + name + nextId;
        String pageBuilderFieldName = "pageBuilder_" + name + nextId;

        // generate adapted method
        List<Class<?>> originalParameterTypes = implementation.getMethodHandle().type().parameterList();
        List<Class<?>> adaptedParameterTypes = new ArrayList<>();
        Class adaptedReturnType;
        MethodType adaptedMethodType;
        List<Parameter> adaptedParameterList = new ArrayList<>();

        if (writeToOutputBlock) {
            adaptedParameterList.add(arg("outputBlock", BlockBuilder.class));
            adaptedParameterTypes.add(BlockBuilder.class);
        }

        int numSkippedParameters = implementation.getDependentFunctionInfos().size();
        if (implementation.isWriteToOutputBlock()) {
            numSkippedParameters++;
        }

        for (int i = numSkippedParameters; i < originalParameterTypes.size(); i++) {
            int argumentIndex = i - numSkippedParameters;
            adaptedParameterList.add(arg("arg" + argumentIndex, type(originalParameterTypes.get(i))));
            adaptedParameterTypes.add(originalParameterTypes.get(i));
        }

        if (writeToOutputBlock) {
            // adapted method should directly write to the output block
            adaptedReturnType = void.class;
        }
        else {
            // adapted method will return the value on stack
            if (implementation.getMethodHandle().type().returnType() != void.class) {
                // hack!!!
                adaptedReturnType = implementation.getMethodHandle().type().returnType();
            }
            else {
                adaptedReturnType = outputType.get().getJavaType();
            }

            if (implementation.isNullable()) {
                adaptedReturnType = Primitives.wrap(adaptedReturnType);
            }
        }

        MethodDefinition adaptedMethod;
        adaptedMethod = classDefinition.declareMethod(a(PUBLIC), fieldAndMethodName, type(adaptedReturnType), adaptedParameterList);
        adaptedMethodType = MethodType.methodType(adaptedReturnType, adaptedParameterTypes);

        BytecodeBlock body = adaptedMethod.getBody();
        Variable thisVariable = adaptedMethod.getThis();
        Optional<Variable> outputBlockVariable = Optional.empty();

        // Step 1. push the output block if the scalar function implementation needs it.
        if (implementation.isWriteToOutputBlock()) {
            checkState(implementation.getMethodHandle().type().parameterList().get(0) == BlockBuilder.class);

            // prepare the output block for the method
            if (writeToOutputBlock) {
                // easy case, just forward the output block provided to the adapted method
                outputBlockVariable = Optional.of(adaptedParameterList.get(0));
                body.append(adaptedParameterList.get(0));
            }
            else {
                // get page builder first
                FieldDefinition pageBuilderField = createPageBuilderField(pageBuilderFieldName, outputType.get());
                Variable pageBuilderVariable = adaptedMethod.getScope().declareVariable(PageBuilder.class, "pageBuilder");
                body.comment("get and prepare PageBuilder")
                        .append(pageBuilderVariable.set(thisVariable.getField(pageBuilderField)))
                        .append(new IfStatement()
                                .condition(pageBuilderVariable.invoke("isFull", boolean.class))
                                .ifTrue(pageBuilderVariable.invoke("reset", void.class)));

                // get block builder from page builder
                outputBlockVariable = Optional.of(adaptedMethod.getScope().declareVariable(Block.class, "outputBlock"));
                body.comment("get BlockBuilder from PageBuilder")
                        .append(outputBlockVariable.get().set(pageBuilderVariable.invoke("getBlockBuilder", BlockBuilder.class, constantInt(0))))
                        .append(outputBlockVariable.get());
            }
        }

        // Step 2. push the dependent functions
        for (DependentFunctionInfo functionInfo : implementation.getDependentFunctionInfos()) {
            FieldDefinition adaptedDependentFunction = getAdaptedScalarFunction(functionInfo.getName(), functionInfo.getImplementation(), false, Optional.of(functionInfo.getOutputType())).getField();
            adaptedMethod.getBody().append(adaptedMethod.getThis().getField(adaptedDependentFunction));
        }

        // Step 3. forward all the parameters presented to the adapted method
        List<Parameter> forwardParameters = adaptedParameterList;
        if (writeToOutputBlock) {
            forwardParameters = forwardParameters.subList(1, forwardParameters.size());
        }
        for (Parameter parameter : forwardParameters) {
            body.append(parameter);
        }

        // Step 4. invoke the function implementation
        body.append(invoke(callSiteBinder.bind(implementation.getMethodHandle()), name));

        // Step 5. do the output adaption
//        Class<?> valueJavaType = outputType.get().getJavaType();
        Class<?> valueJavaType = Primitives.unwrap(adaptedReturnType);
        if (!valueJavaType.isPrimitive() && valueJavaType != Slice.class) {
            valueJavaType = Object.class;
        }

        // Step 5a. if the adapted method is expected to write to output block,
        // but the scalar function implementation returns the value on stack, adapt it.
        if (writeToOutputBlock && !implementation.isWriteToOutputBlock()) {
            //  result is on the stack, append it to the output BlockBuilder
            Variable tempValue = adaptedMethod.getScope().createTempVariable(valueJavaType);

            if (!implementation.isNullable()) {
                String methodName = "write" + Primitives.wrap(valueJavaType).getSimpleName();
                body.comment("%s.%s(output, %s)", outputType.get().getTypeSignature(), methodName, valueJavaType.getSimpleName())
                        .putVariable(tempValue)
                        .append(loadConstant(callSiteBinder.bind(outputType.get(), Type.class)))
                        .getVariable(outputBlockVariable.get())
                        .getVariable(tempValue)
                        .invokeInterface(Type.class, methodName, void.class, BlockBuilder.class, valueJavaType);
            }
            else {
                throw new NotSupportedException("Not Implemented Yet");
            }
        }

        // Step 5b. if the adapted method is expected to return the value on stack,
        // but the scalar function implementation writes to output block, adapt it.
        if (!writeToOutputBlock && implementation.isWriteToOutputBlock()) {
            String methodName = "get" + Primitives.wrap(valueJavaType).getSimpleName();

            if (!implementation.isNullable()) {
                body.append(new BytecodeBlock()
                        .setDescription("slice the result on stack")
                        .append(new BytecodeBlock()
                                .comment("%s.%s(outputBlock.getPositionCount() - 1)", outputType.get().getTypeSignature(), methodName)
                                .append(constantType(callSiteBinder, outputType.get())
                                        .invoke(
                                                methodName,
                                                valueJavaType,
                                                outputBlockVariable.get().cast(Block.class),
                                                subtract(outputBlockVariable.get().invoke("getPositionCount", int.class), constantInt(1)))
                                        .cast(outputType.get().getJavaType()))));
            }
            else {
                throw new NotSupportedException("Not Implemented Yet");
                /*
            body.append(new BytecodeBlock()
                    .setDescription("slice the result on stack")
                    .comment("if outputBlock.isNull(outputBlock.getPositionCount() - 1)")
                    .append(new IfStatement()
                            .condition(outputBlock.get().invoke("isNull", boolean.class, subtract(outputBlock.get().invoke("getPositionCount", int.class), constantInt(1))))
                            .ifTrue(new BytecodeBlock()
                                    .comment("loadJavaDefault(%s); wasNull = true", outputType.get().getJavaType().getName())
                                    .pushJavaDefault(outputType.get().getJavaType())
                                    .append(scope.getVariable("wasNull").set(constantTrue())))
                            .ifFalse(new BytecodeBlock()
                                    .comment("%s.%s(outputBlock.getPositionCount() - 1)", outputType.get().getTypeSignature(), methodName)
                                    .append(constantType(binder, outputType.get())
                                            .invoke(
                                                    methodName,
                                                    valueJavaType,
                                                    outputBlock.get().cast(Block.class),
                                                    subtract(outputBlock.get().invoke("getPositionCount", int.class), constantInt(1)))
                                            .cast(outputType.get().getJavaType())))));
                                            */
            }
        }

        // Step 6. Return
        body.ret(adaptedReturnType);

        // define the instance and static field
        FieldDefinition staticField = classDefinition.declareField(a(PRIVATE, STATIC, FINAL), fieldAndMethodName, MethodHandle.class);
        FieldDefinition instanceField = classDefinition.declareField(a(PRIVATE, FINAL), "binded_" + fieldAndMethodName, MethodHandle.class);

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
                                        adaptedParameterList.stream()
                                                .map(Parameter::getType)
                                                .map(BytecodeExpressions::constantClass)
                                                .collect(toImmutableList())))));

        adaptedScalarFunctionFields.add(new AdaptedScalarFunctionField(staticField, instanceField));
        nextId++;
        return new AdaptedScalarFunction(instanceField, adaptedMethodType);
    }

    private FieldDefinition createPageBuilderField(String pageBuilderFieldName, Type type)
    {
        FieldDefinition field = classDefinition.declareField(a(PRIVATE, FINAL), pageBuilderFieldName, PageBuilder.class);
        pageBuilderFields.put(field, type);
        return field;
    }

    public void generateInitializations(Variable thisVariable, BytecodeBlock block)
    {
        for (Map.Entry<FieldDefinition, Type> entry : pageBuilderFields.entrySet()) {
            FieldDefinition field = entry.getKey();
            Type type = entry.getValue();

            block.append(thisVariable)
                    .append(constantType(callSiteBinder, type))
                    .append(invoke(callSiteBinder.bind(CREATE_PAGE_BUILDER_METHOD_HANDLE), "createPageBuilder"))
                    .putField(field);

//                  TODO: why the previous way doesn't work???

//            block.append(thisVariable.setField())
//            block.append(newInstance(PageBuilder.class, constantType(callSiteBinder, type)))
//                    .putField(field);

            /*
            block.append(
                    thisVariable.setField(
                            field,
                            newInstance(
                                    PageBuilder.class,
                                    loadConstant(callSiteBinder, ImmutableList.of(type), ImmutableList.of(type).getClass()))));
            */
        }

        for (AdaptedScalarFunctionField adaptedFunctionField : adaptedScalarFunctionFields) {
            adaptedFunctionField.generateInitialization(thisVariable, block);
        }
    }

    static class AdaptedScalarFunctionField
    {
        private final FieldDefinition staticField;
        // the instance field will be binded to "this" in constructor
        private final FieldDefinition instanceField;

        public AdaptedScalarFunctionField(FieldDefinition staticField, FieldDefinition instanceField)
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

    public static class AdaptedScalarFunction
    {
        private final FieldDefinition field;
        private final MethodType adaptedMethodType;

        public AdaptedScalarFunction(FieldDefinition field, MethodType adaptedMethodType)
        {
            this.field = field;
            this.adaptedMethodType = adaptedMethodType;
        }

        public FieldDefinition getField()
        {
            return field;
        }

        public MethodType getAdaptedMethodType()
        {
            return adaptedMethodType;
        }
    }

    public static PageBuilder createPageBuilder(Type type)
    {
        return new PageBuilder(ImmutableList.of(type));
    }
}
