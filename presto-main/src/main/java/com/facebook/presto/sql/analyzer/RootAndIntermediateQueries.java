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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public final class RootAndIntermediateQueries
{
    private final List<MaterializedIntermediateExtractor.WithQueryQualifiedName> withQueryQualifiedNames = new ArrayList<>();
    private Statement statement;

    public void addWithQuery(CatalogSchemaTableName catalogSchemaName, WithQuery node)
    {
        withQueryQualifiedNames.add(new MaterializedIntermediateExtractor.WithQueryQualifiedName(catalogSchemaName, node));
    }

    public void setRootStatement(Statement statement)
    {
        this.statement = statement;
    }

    public List<MaterializedIntermediateExtractor.WithQueryQualifiedName> getWithQueries()
    {
        return withQueryQualifiedNames;
    }

    public Statement getRootStatement()
    {
        return requireNonNull(statement, "Statement was null");
    }
}
