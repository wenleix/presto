package com.facebook.presto.sql.gen;

import com.facebook.presto.sql.relational.CallExpression;
import com.facebook.presto.sql.relational.ConstantExpression;
import com.facebook.presto.sql.relational.InputReferenceExpression;
import com.facebook.presto.sql.relational.LambdaDefinitionExpression;
import com.facebook.presto.sql.relational.RowExpressionVisitor;
import com.facebook.presto.sql.relational.VariableReferenceExpression;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;

public class AggregationLambdaBytecodeGenerator
{
    public static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler()
    {
        return new RowExpressionVisitor<BytecodeNode, Scope>()
        {
            @Override
            public BytecodeNode visitCall(CallExpression call, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitInputReference(InputReferenceExpression reference, Scope context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytecodeNode visitConstant(ConstantExpression literal, Scope context)
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
                throw new UnsupportedOperationException();
            }
        };
    }
}
