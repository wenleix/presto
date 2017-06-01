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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.ArrayBlockBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.BooleanOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.VarcharOperators;
import com.facebook.presto.util.JsonUtil;
import com.facebook.presto.util.JsonUtil.JsonToBlockAppender;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static com.facebook.presto.type.TypeJsonUtils.canCastFromJson;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.JSON_FACTORY;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class JsonToArrayCast
        extends SqlOperator
{
    public static final JsonToArrayCast JSON_TO_ARRAY = new JsonToArrayCast();
    private static final MethodHandle METHOD_HANDLE = methodHandle(JsonToArrayCast.class, "toArray", Type.class, JsonToBlockAppender.class, ConnectorSession.class, Slice.class);

    private JsonToArrayCast()
    {
        super(OperatorType.CAST,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature(StandardTypes.JSON)));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type type = boundVariables.getTypeVariable("T");
        Type arrayType = typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(TypeSignatureParameter.of(type.getTypeSignature())));
        checkCondition(canCastFromJson(arrayType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", arrayType);

        JsonToBlockAppender elementAppender = JsonToBlockAppender.createJsonToBlockAppender(arrayType.getTypeParameters().get(0));
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(arrayType).bindTo(elementAppender);
        return new ScalarFunctionImplementation(true, ImmutableList.of(false), methodHandle, isDeterministic());
    }

    @UsedByGeneratedCode
    public static Block toArray(Type arrayType, JsonToBlockAppender elementAppender, ConnectorSession connectorSession, Slice json)
    {
        Type elementType = arrayType.getTypeParameters().get(0);

        try (JsonParser jsonParser = JsonUtil.createJsonParser(JSON_FACTORY, json)) {
            BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), 20);

            jsonParser.nextToken();
            if (jsonParser.getCurrentToken() == JsonToken.VALUE_NULL) {
                return null;
            }
            checkState(jsonParser.currentToken() == JsonToken.START_ARRAY, "Expected a json array");
            while (jsonParser.nextValue() != JsonToken.END_ARRAY) {
                elementAppender.parseAndAppendBlock(jsonParser, blockBuilder);
//                cheatAppendBigintValue(jsonParser, blockBuilder);
            }

            return blockBuilder.build();
        }
        catch (RuntimeException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to " + arrayType, e);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    // for performance benchmark only
    private static void cheatAppendBigintValue(JsonParser jsonParser, BlockBuilder blockBuilder)
            throws IOException
    {
        Long result;
        switch (jsonParser.getCurrentToken()) {
            case VALUE_NULL:
                result = null;
                break;
            case VALUE_STRING:
                result = VarcharOperators.castToBigint(Slices.utf8Slice(jsonParser.getText()));
                break;
            case VALUE_NUMBER_FLOAT:
                result = DoubleOperators.castToLong(jsonParser.getDoubleValue());
                break;
            case VALUE_NUMBER_INT:
                result = jsonParser.getLongValue();
                break;
            case VALUE_TRUE:
                result = BooleanOperators.castToBigint(true);
                break;
            case VALUE_FALSE:
                result = BooleanOperators.castToBigint(false);
                break;
            default:
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to %s", jsonParser.getCurrentValue().toString(), BIGINT));
        }
        if (result == null) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, result);
        }
    }
}
