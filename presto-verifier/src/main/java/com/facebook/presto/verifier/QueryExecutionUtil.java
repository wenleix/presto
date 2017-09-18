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
package com.facebook.presto.verifier;

import com.facebook.presto.jdbc.PrestoConnection;
import com.facebook.presto.jdbc.PrestoStatement;
import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.spi.type.SqlVarbinary;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Collections.unmodifiableList;

public class QueryExecutionUtil
{
    private QueryExecutionUtil() {}

    public static QueryResult executeQuery(
            String url,
            String username,
            String password,
            Query query,
            String sql,
            Duration timeout,
            int maxRowCount,
            Map<String, String> sessionProperties,
            Map<String, String> clientInfo)
    {
        String queryId = null;
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            trySetConnectionProperties(query, connection, clientInfo);
            for (Map.Entry<String, String> entry : sessionProperties.entrySet()) {
                connection.unwrap(PrestoConnection.class).setSessionProperty(entry.getKey(), entry.getValue());
            }

            try (Statement statement = connection.createStatement()) {
                TimeLimiter limiter = new SimpleTimeLimiter();
                Stopwatch stopwatch = Stopwatch.createStarted();
                Statement limitedStatement = limiter.newProxy(statement, Statement.class, timeout.toMillis(), TimeUnit.MILLISECONDS);
                long start = System.nanoTime();
                PrestoStatement prestoStatement = limitedStatement.unwrap(PrestoStatement.class);
                ProgressMonitor progressMonitor = new ProgressMonitor();
                prestoStatement.setProgressMonitor(progressMonitor);
                try {
                    boolean isSelectQuery = limitedStatement.execute(sql);
                    List<List<Object>> results = null;
                    if (isSelectQuery) {
                        results = limiter.callWithTimeout(
                                getResultSetConverter(limitedStatement.getResultSet(), maxRowCount),
                                timeout.toMillis() - stopwatch.elapsed(TimeUnit.MILLISECONDS),
                                TimeUnit.MILLISECONDS, true);
                    }
                    else {
                        results = ImmutableList.of(ImmutableList.of(limitedStatement.getLargeUpdateCount()));
                    }
                    prestoStatement.clearProgressMonitor();
                    QueryStats queryStats = progressMonitor.getFinalQueryStats();
                    if (queryStats == null) {
                        throw new VerifierException("Cannot fetch query stats");
                    }
                    Duration queryCpuTime = new Duration(queryStats.getCpuTimeMillis(), TimeUnit.MILLISECONDS);
                    queryId = queryStats.getQueryId();
                    return new QueryResult(QueryResult.State.SUCCESS, null, nanosSince(start), queryCpuTime, queryId, results);
                }
                catch (AssertionError e) {
                    if (e.getMessage().startsWith("unimplemented type:")) {
                        return new QueryResult(QueryResult.State.INVALID, null, null, null, queryId, ImmutableList.of());
                    }
                    throw e;
                }
                catch (SQLException | VerifierException e) {
                    throw e;
                }
                catch (UncheckedTimeoutException e) {
                    return new QueryResult(QueryResult.State.TIMEOUT, null, null, null, queryId, ImmutableList.of());
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
                catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        }
        catch (SQLException e) {
            Exception exception = e;
            if (("Error executing query".equals(e.getMessage()) || "Error fetching results".equals(e.getMessage())) &&
                    (e.getCause() instanceof Exception)) {
                exception = (Exception) e.getCause();
            }
            QueryResult.State state = isPrestoQueryInvalid(e) ? QueryResult.State.INVALID : QueryResult.State.FAILED;
            return new QueryResult(state, exception, null, null, null, null);
        }
        catch (VerifierException e) {
            return new QueryResult(QueryResult.State.TOO_MANY_ROWS, e, null, null, null, null);
        }
    }

    private static void trySetConnectionProperties(Query query, Connection connection, Map<String, String> clientInfo)
            throws SQLException
    {
        // Required for jdbc drivers that do not implement all/some of these functions (eg. impala jdbc driver)
        // For these drivers, set the database default values in the query database
        try {
            for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
                connection.setClientInfo(entry.getKey(), entry.getValue());
            }
            connection.setCatalog(query.getCatalog());
            connection.setSchema(query.getSchema());
        }
        catch (SQLClientInfoException ignored) {
            // Do nothing
        }
    }

    private static boolean isPrestoQueryInvalid(SQLException e)
    {
        for (Throwable t = e.getCause(); t != null; t = t.getCause()) {
            if (t.toString().contains(".SemanticException:")) {
                return true;
            }
            if (t.toString().contains(".ParsingException:")) {
                return true;
            }
            if (nullToEmpty(t.getMessage()).matches("Function .* not registered")) {
                return true;
            }
        }
        return false;
    }

    private static Callable<List<List<Object>>> getResultSetConverter(ResultSet resultSet, int maxRowCount)
    {
        return () -> convertJdbcResultSet(resultSet, maxRowCount);
    }

    private static List<List<Object>> convertJdbcResultSet(ResultSet resultSet, int maxRowCount)
            throws SQLException, VerifierException
    {
        int rowCount = 0;
        int columnCount = resultSet.getMetaData().getColumnCount();

        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        while (resultSet.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                Object object = resultSet.getObject(i);
                if (object instanceof BigDecimal) {
                    if (((BigDecimal) object).scale() <= 0) {
                        object = ((BigDecimal) object).longValueExact();
                    }
                    else {
                        object = ((BigDecimal) object).doubleValue();
                    }
                }
                if (object instanceof Array) {
                    object = ((Array) object).getArray();
                }
                if (object instanceof byte[]) {
                    object = new SqlVarbinary((byte[]) object);
                }
                row.add(object);
            }
            rows.add(unmodifiableList(row));
            rowCount++;
            if (rowCount > maxRowCount) {
                throw new VerifierException("More than '" + maxRowCount + "' rows, failing query");
            }
        }
        return rows.build();
    }

    private static class ProgressMonitor
            implements Consumer<QueryStats>
    {
        private QueryStats queryStats;
        private boolean finished = false;

        @Override
        public synchronized void accept(QueryStats queryStats)
        {
            checkState(!finished);
            this.queryStats = queryStats;
        }

        public synchronized QueryStats getFinalQueryStats()
        {
            finished = true;
            return queryStats;
        }
    }
}
