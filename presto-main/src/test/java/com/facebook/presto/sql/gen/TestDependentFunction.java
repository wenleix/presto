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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation.DependentFunctionInfo;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.internalScalarFunction;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.gen.TestDependentFunction.TestIdentity2.IDENTITY2;
import static com.facebook.presto.sql.gen.TestWriteToOutputBlock.TestIdentity.IDENTITY;
import static com.facebook.presto.sql.gen.TestWriteToOutputBlock.TestNullSwitch.NULL_SWITCH;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;

public class TestDependentFunction
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalarFunction(IDENTITY);
        registerScalarFunction(IDENTITY2);
    }

    @Test
    public void test()
            throws Exception
    {
        assertFunction("identity2(123)", INTEGER, 123);
        assertFunction("identity2(identity2(123))", INTEGER, 123);
        assertFunction("identity(CAST(null AS INTEGER))", INTEGER, null);
        assertFunction("identity(identity(CAST(null AS INTEGER)))", INTEGER, null);

        assertFunction("identity(123.4)", DOUBLE, 123.4);
        assertFunction("identity(identity(123.4))", DOUBLE, 123.4);
        assertFunction("identity(CAST(null AS DOUBLE))", DOUBLE, null);
        assertFunction("identity(identity(CAST(null AS DOUBLE)))", DOUBLE, null);

        assertFunction("identity(true)", BOOLEAN, true);
        assertFunction("identity(identity(true))", BOOLEAN, true);
        assertFunction("identity(CAST(null AS BOOLEAN))", BOOLEAN, null);
        assertFunction("identity(identity(CAST(null AS BOOLEAN)))", BOOLEAN, null);

        assertFunction("identity('abc')", createVarcharType(3), "abc");
        assertFunction("identity(identity('abc'))", createVarcharType(3), "abc");
        assertFunction("identity(CAST(null AS VARCHAR))", VARCHAR, null);
        assertFunction("identity(identity(CAST(null AS VARCHAR)))", VARCHAR, null);

        assertFunction("identity(ARRAY[1,2,3])", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("identity(identity(ARRAY[1,2,3]))", new ArrayType(INTEGER), ImmutableList.of(1, 2, 3));
        assertFunction("identity(CAST(null AS ARRAY<INTEGER>))", new ArrayType(INTEGER), null);
        assertFunction("identity(identity(CAST(null AS ARRAY<INTEGER>)))", new ArrayType(INTEGER), null);

    }

    public static class TestIdentity
            extends SqlScalarFunction
    {
        public static final TestIdentity IDENTITY = new TestIdentity();
        private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(TestIdentity.class, "identityLong", Type.class, BlockBuilder.class, long.class);
        private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(TestIdentity.class, "identityDouble", Type.class, BlockBuilder.class, double.class);
        private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(TestIdentity.class, "identityBoolean", Type.class, BlockBuilder.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(TestIdentity.class, "identitySlice", Type.class, BlockBuilder.class, Slice.class);
        private static final MethodHandle METHOD_HANDLE_BLOCK = methodHandle(TestIdentity.class, "identityBlock", Type.class, BlockBuilder.class, Block.class);

        private TestIdentity()
        {
            super(new Signature(
                    "identity",
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature("T"),
                    ImmutableList.of(parseTypeSignature("T")),
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
            return "return the same value";
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            Type type = boundVariables.getTypeVariable("T");
            MethodHandle methodHandle = null;
            if (type.getJavaType() == long.class) {
                methodHandle = METHOD_HANDLE_LONG.bindTo(type);
            }
            else if (type.getJavaType() == double.class) {
                methodHandle = METHOD_HANDLE_DOUBLE.bindTo(type);
            }
            else if (type.getJavaType() == boolean.class) {
                methodHandle = METHOD_HANDLE_BOOLEAN.bindTo(type);
            }
            else if (type.getJavaType() == Slice.class) {
                methodHandle = METHOD_HANDLE_SLICE.bindTo(type);
            }
            else if (type.getJavaType() == Block.class) {
                methodHandle = METHOD_HANDLE_BLOCK.bindTo(type);
            }
            else {
                checkState(false);
            }

            return new ScalarFunctionImplementation(
                    false,
                    ImmutableList.of(false),
                    ImmutableList.of(false),
                    ImmutableList.of(Optional.empty()),
                    methodHandle,
                    Optional.empty(),
                    true,
                    ImmutableList.of(),
                    isDeterministic());
        }

        public static void identityLong(Type type, BlockBuilder outputBlock, long value)
        {
            type.writeLong(outputBlock, value);
        }

        public static void identityDouble(Type type, BlockBuilder outputBlock, double value)
        {
            type.writeDouble(outputBlock, value);
        }

        public static void identityBoolean(Type type, BlockBuilder outputBlock, boolean value)
        {
            type.writeBoolean(outputBlock, value);
        }

        public static void identitySlice(Type type, BlockBuilder outputBlock, Slice value)
        {
            type.writeSlice(outputBlock, value);
        }

        public static void identityBlock(Type type, BlockBuilder outputBlock, Block value)
        {
            type.writeObject(outputBlock, value);
        }
    }

    public static class TestIdentity2
            extends SqlScalarFunction
    {
        public static final TestIdentity2 IDENTITY2 = new TestIdentity2();

        private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(TestIdentity.class, "identityLong", Type.class, BlockBuilder.class, long.class);
        private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(TestIdentity.class, "identityDouble", Type.class, BlockBuilder.class, double.class);
        private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(TestIdentity.class, "identityBoolean", Type.class, BlockBuilder.class, boolean.class);
        private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(TestIdentity.class, "identitySlice", Type.class, BlockBuilder.class, Slice.class);
        private static final MethodHandle METHOD_HANDLE_BLOCK = methodHandle(TestIdentity.class, "identityBlock", Type.class, BlockBuilder.class, Block.class);


        private TestIdentity2()
        {
            super(new Signature(
                    "identity2",
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature("T"),
                    ImmutableList.of(parseTypeSignature("T")),
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
            return "return the same value";
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            Type type = boundVariables.getTypeVariable("T");
            MethodHandle methodHandle = null;

            Signature dependentSignature = internalScalarFunction("identity", type.getTypeSignature(), type.getTypeSignature());
            ScalarFunctionImplementation dependentFunctionImplementation = functionRegistry.getScalarFunctionImplementation(dependentSignature);

            return new ScalarFunctionImplementation(
                    false,
                    ImmutableList.of(false),
                    ImmutableList.of(false),
                    ImmutableList.of(Optional.empty()),
                    methodHandle,
                    Optional.empty(),
                    true,
                    ImmutableList.of(new DependentFunctionInfo("identity", dependentFunctionImplementation, type)),
                    isDeterministic());
        }

        public static long identityLong(MethodHandle identity, long value)
                throws Throwable
        {
            return (long) identity.invokeExact(value);
        }

        public static double identityDouble(MethodHandle identity, double value)
                throws Throwable
        {
            return (double) identity.invokeExact(value);
    }

        public static boolean identityBoolean(MethodHandle identity, boolean value)
                throws Throwable
        {
            return (boolean) identity.invokeExact(value);
        }

        public static Slice identitySlice(MethodHandle identity, Slice value)
                throws Throwable
        {
            return (Slice) identity.invokeExact(value);
        }

        public static Block identityBlock(MethodHandle identity, Block value)
                throws Throwable
        {
            return (Block) identity.invokeExact(value);
        }
    }
}
