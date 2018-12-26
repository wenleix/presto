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

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Variable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.gen.BytecodeUtils.generateWrite;

public abstract class BytecodeGenerator
{
    public BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments, Optional<Variable> outputBlock)
    {
        BytecodeBlock expressionBlock = new BytecodeBlock().append(generateExpression(signature, context, returnType, arguments));
        if (outputBlock.isPresent()) {
            expressionBlock.append(generateWrite(
                    context.getCallSiteBinder(),
                    context.getScope(),
                    context.getScope().getVariable("wasNull"),
                    returnType,
                    outputBlock.get()));
        }

        return expressionBlock;
    }

    protected abstract BytecodeNode generateExpression(Signature signature, BytecodeGeneratorContext context, Type returnType, List<RowExpression> arguments);
}
