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
import com.facebook.presto.bytecode.Variable;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static java.util.Objects.requireNonNull;

public final class CachedInstanceBinder
{
    private final ClassDefinition classDefinition;
    private final Map<FieldDefinition, Binding> initializers = new HashMap<>();
    private int nextId;

    public CachedInstanceBinder(ClassDefinition classDefinition)
    {
        this.classDefinition = requireNonNull(classDefinition, "classDefinition is null");
    }

    public FieldDefinition getCachedInstance(CallSiteBinder callSiteBinder, MethodHandle methodHandle)
    {
        FieldDefinition field = classDefinition.declareField(a(PRIVATE, FINAL), "__cachedInstance" + nextId, methodHandle.type().returnType());
        Binding binding = callSiteBinder.bind(methodHandle);
        initializers.put(field, binding);
        nextId++;
        return field;
    }

    public void generateInitializations(Variable thisVariable, BytecodeBlock block)
    {
        for (Map.Entry<FieldDefinition, Binding> entry : initializers.entrySet()) {
            block.append(thisVariable)
                    .append(invoke(entry.getValue(), "instanceFieldConstructor"))
                    .putField(entry.getKey());
        }
    }
}
