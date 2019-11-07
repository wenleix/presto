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
package com.facebook.presto.spark.planner;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.QueryPreparer.PreparedQuery;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanOptimizers;

import javax.inject.Inject;

import java.util.Optional;

import static com.facebook.presto.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static java.util.Objects.requireNonNull;

public class SparkQueryPlanner
{
    private final SqlParser sqlParser;
    private final PlanOptimizers optimizers;
    private final QueryExplainer queryExplainer;
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final AccessControl accessControl;

    @Inject
    public SparkQueryPlanner(
            SqlParser sqlParser,
            PlanOptimizers optimizers,
            QueryExplainer queryExplainer,
            Metadata metadata,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            AccessControl accessControl)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.optimizers = requireNonNull(optimizers, "optimizers is null");
        this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    public Plan createQueryPlan(Session session, PreparedQuery preparedQuery, WarningCollector warningCollector)
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

        Analyzer analyzer = new Analyzer(
                session,
                metadata,
                sqlParser,
                accessControl,
                Optional.of(queryExplainer),
                preparedQuery.getParameters(),
                warningCollector);

        LogicalPlanner logicalPlanner = new LogicalPlanner(
                false,
                session,
                optimizers.get(),
                idAllocator,
                metadata,
                sqlParser,
                statsCalculator,
                costCalculator,
                warningCollector);

        Analysis analysis = analyzer.analyze(preparedQuery.getStatement());
        return logicalPlanner.plan(analysis, OPTIMIZED_AND_VALIDATED);
    }
}
