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

import com.google.common.base.Throwables;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;

import static com.google.common.base.Throwables.throwIfUnchecked;

public final class LambdaCapture
{
    public static final Method LAMBDA_CAPTURE_METHOD;

    static {
        try {
            LAMBDA_CAPTURE_METHOD = LambdaCapture.class.getMethod("lambdaCapture", MethodHandles.Lookup.class, String.class, MethodType.class, MethodType.class, MethodHandle.class, MethodType.class);
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    private LambdaCapture()
    {
    }

    public static CallSite lambdaCapture(
            MethodHandles.Lookup callerLookup,
            String name,
            MethodType type,
            MethodType samMethodType,
            MethodHandle implMethod,
            MethodType instantiatedMethodType)
    {
        try {
            // Currently delegate to metafactory
            return LambdaMetafactory.metafactory(
                    callerLookup,
                    name,
                    type,
                    samMethodType,
                    implMethod,
                    instantiatedMethodType);
        }
        catch (LambdaConversionException e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
