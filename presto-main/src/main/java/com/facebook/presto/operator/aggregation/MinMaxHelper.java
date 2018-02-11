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

import com.facebook.presto.operator.aggregation.state.BlockState;
import com.facebook.presto.operator.aggregation.state.NullableBooleanState;
import com.facebook.presto.operator.aggregation.state.NullableDoubleState;
import com.facebook.presto.operator.aggregation.state.NullableLongState;
import com.facebook.presto.operator.aggregation.state.ObjectBlockPositionState;
import com.facebook.presto.operator.aggregation.state.SliceBlockPositionState;
import com.facebook.presto.operator.aggregation.state.SliceState;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.util.Failures.internalError;

public class MinMaxHelper
{
    private MinMaxHelper()
    {}

    public static void combineStateWithValue(MethodHandle comparator, NullableDoubleState state, double value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(value);
            return;
        }
        try {
            if ((boolean) comparator.invokeExact(value, state.getDouble())) {
                state.setDouble(value);
            }
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    public static void combineStateWithValue(MethodHandle comparator, NullableLongState state, long value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(value);
            return;
        }
        try {
            if ((boolean) comparator.invokeExact(value, state.getLong())) {
                state.setLong(value);
            }
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    public static void combineStateWithValue(MethodHandle comparator, NullableBooleanState state, boolean value)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setBoolean(value);
            return;
        }
        try {
            if ((boolean) comparator.invokeExact(value, state.getBoolean())) {
                state.setBoolean(value);
            }
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    public static void minCombineStateWithValue(Type type, SliceBlockPositionState state, Block block, int position)
    {
        if (state.getBlock() == null) {
            state.setBlock(block);
            state.setPosition(position);
            return;
        }
        if (type.compareTo(block, position, state.getBlock(), state.getPosition()) < 0) {
            state.setBlock(block);
            state.setPosition(position);
        }
    }

    public static void maxCombineStateWithValue(Type type, SliceBlockPositionState state, Block block, int position)
    {
        if (state.getBlock() == null) {
            state.setBlock(block);
            state.setPosition(position);
            return;
        }
        if (type.compareTo(block, position, state.getBlock(), state.getPosition()) > 0) {
            state.setBlock(block);
            state.setPosition(position);
        }
    }

    public static void minCombineStateWithValue(Type type, ObjectBlockPositionState state, Block block, int position)
    {
        if (state.getBlock() == null) {
            state.setBlock(block);
            state.setPosition(position);
            return;
        }
        if (type.compareTo(block, position, state.getBlock(), state.getPosition()) < 0) {
            state.setBlock(block);
            state.setPosition(position);
        }
    }

    public static void maxCombineStateWithValue(Type type, ObjectBlockPositionState state, Block block, int position)
    {
        if (state.getBlock() == null) {
            state.setBlock(block);
            state.setPosition(position);
            return;
        }
        if (type.compareTo(block, position, state.getBlock(), state.getPosition()) > 0) {
            state.setBlock(block);
            state.setPosition(position);
        }
    }

    public static void combineStateWithState(MethodHandle comparator, NullableDoubleState state, NullableDoubleState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setDouble(otherState.getDouble());
            return;
        }
        try {
            if ((boolean) comparator.invokeExact(otherState.getDouble(), state.getDouble())) {
                state.setDouble(otherState.getDouble());
            }
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    public static void combineStateWithState(MethodHandle comparator, NullableLongState state, NullableLongState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setLong(otherState.getLong());
            return;
        }
        try {
            if ((boolean) comparator.invokeExact(otherState.getLong(), state.getLong())) {
                state.setLong(otherState.getLong());
            }
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    public static void combineStateWithState(MethodHandle comparator, NullableBooleanState state, NullableBooleanState otherState)
    {
        if (state.isNull()) {
            state.setNull(false);
            state.setBoolean(otherState.getBoolean());
            return;
        }
        try {
            if ((boolean) comparator.invokeExact(otherState.getBoolean(), state.getBoolean())) {
                state.setBoolean(otherState.getBoolean());
            }
        }
        catch (Throwable t) {
            throw internalError(t);
        }
    }

    public static void minCombineStateWithState(Type type, SliceBlockPositionState state, SliceBlockPositionState otherState)
    {
        if (state.getBlock() == null) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
            return;
        }
        if (type.compareTo(otherState.getBlock(), otherState.getPosition(), state.getBlock(), state.getPosition()) < 0) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
        }
    }

    public static void maxCombineStateWithState(Type type, SliceBlockPositionState state, SliceBlockPositionState otherState)
    {
        if (state.getBlock() == null) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
            return;
        }
        if (type.compareTo(otherState.getBlock(), otherState.getPosition(), state.getBlock(), state.getPosition()) > 0) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
        }
    }

    public static void minCombineStateWithState(Type type, ObjectBlockPositionState state, ObjectBlockPositionState otherState)
    {
        if (state.getBlock() == null) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
            return;
        }
        if (type.compareTo(otherState.getBlock(), otherState.getPosition(), state.getBlock(), state.getPosition()) < 0) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
        }
    }

    public static void maxCombineStateWithState(Type type, ObjectBlockPositionState state, ObjectBlockPositionState otherState)
    {
        if (state.getBlock() == null) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
            return;
        }
        if (type.compareTo(otherState.getBlock(), otherState.getPosition(), state.getBlock(), state.getPosition()) > 0) {
            state.setBlock(otherState.getBlock());
            state.setPosition(otherState.getPosition());
        }
    }
}
