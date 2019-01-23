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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.CatalogSchemaName;
import com.facebook.presto.spi.CatalogSchemaTableName;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.TO_MATERIALIZE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

public final class MaterializedIntermediateExtractor
{
    private static final Splitter COMMA_SPLITTER = Splitter.on('.');

    private MaterializedIntermediateExtractor() {}

    public static RootAndIntermediateQueries extractRootAndIntermediateQueries(
            Session session,
            Metadata metadata,
            Statement statement)
    {
        RootAndIntermediateQueries referenceCollector = new RootAndIntermediateQueries();
        Optional<CatalogSchemaName> tempCatalogSchema = SystemSessionProperties.getTempCatalogSchema(session);
        if (!tempCatalogSchema.isPresent()) {
            referenceCollector.setRootStatement(statement);
            return referenceCollector;
        }
        Statement rootStatement = (Statement) new Visitor(getNamedQueriesToMaterialize(session), tempCatalogSchema.get()).process(statement, referenceCollector);
        referenceCollector.setRootStatement(rootStatement);
        return referenceCollector;
    }

    private static Set<String> getNamedQueriesToMaterialize(Session session)
    {
        String tablesRaw = session.getSystemProperty(TO_MATERIALIZE, String.class);
        return ImmutableSet.copyOf(COMMA_SPLITTER.split(tablesRaw));
    }

    private static class Visitor
            extends AstVisitor<Node, RootAndIntermediateQueries>
    {
        private final Set<String> namedQueriesToMaterialize;
        private final CatalogSchemaName catalogSchemaName;
        private Map<String, QualifiedName> nameMap = new HashMap<>();

        public Visitor(Set<String> namedQueriesToMaterialize, CatalogSchemaName catalogSchemaName)
        {
            this.namedQueriesToMaterialize = ImmutableSet.copyOf(namedQueriesToMaterialize);
            this.catalogSchemaName = catalogSchemaName;
        }

        @Override
        protected Node visitTable(Table table, RootAndIntermediateQueries context)
        {
            final QualifiedName name;
            if (namedQueriesToMaterialize.contains(table.getName().toString())) {
                name = nameMap.get(table.getName().toString());
            }
            else {
                name = table.getName();
            }
            return table.getLocation()
                    .map(location -> new Table(location, name))
                    .orElseGet(() -> new Table(name));
        }

        @Override
        protected Node visitWithQuery(WithQuery node, RootAndIntermediateQueries context)
        {
            if (namedQueriesToMaterialize.contains(node.getName().getValue())) {
                CatalogSchemaTableName target = new CatalogSchemaTableName(
                        catalogSchemaName.getCatalogName(),
                        catalogSchemaName.getSchemaName(),
                        format("%s_%s", node.getName().getValue(), randomUUID()));
                context.addWithQuery(target, node);
                QualifiedName qualifiedName = QualifiedName.of(target.getCatalogName(), target.getSchemaTableName().getSchemaName(), target.getSchemaTableName().getTableName());
                nameMap.put(node.getName().getValue(), qualifiedName);
            }
            return process(node.getQuery(), context);
        }

        @Override
        protected Node visitQuery(Query node, RootAndIntermediateQueries context)
        {
            Optional<With> optionalWith = node.getWith().flatMap(with -> getWith(node, context, with));
            QueryBody queryBody = (QueryBody) process(node.getQueryBody(), context);
            return node.getLocation()
                    .map(location -> new Query(location, optionalWith, queryBody, node.getOrderBy(), node.getLimit()))
                    .orElseGet(() -> new Query(optionalWith, queryBody, node.getOrderBy(), node.getLimit()));
        }

        private Optional<With> getWith(Query node, RootAndIntermediateQueries context, With with)
        {
            for (WithQuery query : with.getQueries()) {
                process(query, context);
            }

            List<WithQuery> withQueries = with.getQueries().stream()
                    .filter(withQuery -> !namedQueriesToMaterialize.contains(withQuery.getName().getValue()))
                    .collect(toImmutableList());

            if (withQueries.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(node.getLocation()
                    .map(location -> new With(location, with.isRecursive(), withQueries))
                    .orElseGet(() -> new With(with.isRecursive(), withQueries)));
        }

        protected Node visitQuerySpecification(QuerySpecification node, RootAndIntermediateQueries context)
        {
            Optional<Relation> relation = node.getFrom().map(from -> (Relation) process(from, context));
            return node.getLocation()
                    .map(location -> new QuerySpecification(location, node.getSelect(), relation, node.getWhere(), node.getGroupBy(), node.getHaving(), node.getOrderBy(), node.getLimit()))
                    .orElseGet(() -> new QuerySpecification(node.getSelect(), relation, node.getWhere(), node.getGroupBy(), node.getHaving(), node.getOrderBy(), node.getLimit()));
        }

        @Override
        protected Node visitNode(Node node, RootAndIntermediateQueries context)
        {
            return node;
        }
    }

    public static class WithQueryQualifiedName
    {
        private final CatalogSchemaTableName qualifiedName;
        private final WithQuery withQuery;

        public WithQueryQualifiedName(CatalogSchemaTableName qualifiedName, WithQuery withQuery)
        {
            this.qualifiedName = qualifiedName;
            this.withQuery = withQuery;
        }

        public CatalogSchemaTableName getQualifiedName()
        {
            return qualifiedName;
        }

        public WithQuery getWithQuery()
        {
            return withQuery;
        }
    }
}
