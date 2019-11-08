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
package com.facebook.presto.spark;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.PrestoQueryExecution;
import com.facebook.presto.spark.spi.QueryExecutionFactory;
import com.facebook.presto.spark.spi.SessionInfo;
import com.facebook.presto.spark.spi.TaskCompiler;
import com.facebook.presto.spark.spi.TaskCompilerFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class SparkQueryRunner
        implements QueryRunner
{
    private static final Map<String, SparkQueryRunner> instances = new ConcurrentHashMap<>();

    private final Session defaultSession;
    private final int nodeCount;

    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final ConnectorPlanOptimizerManager connectorPlanOptimizerManager;
    private final StatsCalculator statsCalculator;
    private final PluginManager pluginManager;
    private final ConnectorManager connectorManager;

    private final LifeCycleManager lifeCycleManager;

    private final SparkContext sparkContext;
    private final PrestoSparkService prestoSparkService;

    private final String instanceId;

    public SparkQueryRunner(int nodeCount)
            throws IOException
    {
        this.nodeCount = nodeCount;

        defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        PrestoSparkInjectorFactory injectorFactory = new PrestoSparkInjectorFactory(
                ImmutableMap.of(
                        "presto.version", "testversion",
                        "query.hash-partition-count", Integer.toString(nodeCount * 2),
                        "redistribute-writes", "false"),
                ImmutableList.of());

        Injector injector = injectorFactory.create();
        transactionManager = injector.getInstance(TransactionManager.class);
        metadata = injector.getInstance(Metadata.class);
        splitManager = injector.getInstance(SplitManager.class);
        pageSourceManager = injector.getInstance(PageSourceManager.class);
        nodePartitioningManager = injector.getInstance(NodePartitioningManager.class);
        connectorPlanOptimizerManager = injector.getInstance(ConnectorPlanOptimizerManager.class);
        statsCalculator = injector.getInstance(StatsCalculator.class);
        pluginManager = injector.getInstance(PluginManager.class);
        connectorManager = injector.getInstance(ConnectorManager.class);

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        SparkConf sparkConfiguration = new SparkConf()
                .setMaster(format("local[%s]", nodeCount))
                .setAppName("presto");
        sparkContext = new SparkContext(sparkConfiguration);
        prestoSparkService = injector.getInstance(PrestoSparkService.class);

        // Install tpch Plugin
        pluginManager.installPlugin(new TpchPlugin());
        connectorManager.createConnection(
                "tpch",
                "tpch",
                ImmutableMap.of(
                        // TODO: partitioned sources are not supported by Presto on Spark yet
                        "tpch.partitioning-enabled", "false"));

        // Install Hive Plugin, copied from HiveQueryRunner
        File baseDir = createTempDirectory("PrestoTest").toFile();
        System.err.println("Wenlei Debug: " + baseDir);

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());

        FileHiveMetastore metastore = new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
        metastore.createDatabase(createDatabaseMetastoreObject("hive_test"));
        pluginManager.installPlugin(new HivePlugin("hive", Optional.of(metastore)));

        connectorManager.createConnection("hive", "hive", ImmutableMap.of());

        // register the instance
        instanceId = randomUUID().toString();
        instances.put(instanceId, this);
    }

    @Override
    public int getNodeCount()
    {
        return nodeCount;
    }

    @Override
    public Session getDefaultSession()
    {
        return defaultSession;
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    @Override
    public Metadata getMetadata()
    {
        return metadata;
    }

    @Override
    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        return connectorPlanOptimizerManager;
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        return statsCalculator;
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaterializedResult execute(String sql)
    {
        return execute(defaultSession, sql);
    }

    @Override
    public MaterializedResult execute(Session session, String sql)
    {
        QueryExecutionFactory queryExecutionFactory = prestoSparkService.createQueryExecutionFactory();
        PrestoQueryExecution queryExecution = (PrestoQueryExecution) queryExecutionFactory.create(
                sparkContext,
                createSessionInfo(session),
                sql,
                new TestingCompilerFactory(instanceId));
        List<List<Object>> results = queryExecution.execute();
        List<MaterializedRow> rows = results.stream()
                .map(result -> new MaterializedRow(DEFAULT_PRECISION, result))
                .collect(toImmutableList());

        if (queryExecution.getUpdateType() == null) {
            return new MaterializedResult(rows, queryExecution.getOutputTypes());
        }
        else {
            return new MaterializedResult(
                    rows,
                    queryExecution.getOutputTypes(),
                    ImmutableMap.of(),
                    ImmutableSet.of(),
                    Optional.ofNullable(queryExecution.getUpdateType()),
                    OptionalLong.of((Long) getOnlyElement(getOnlyElement(rows).getFields())),
                    ImmutableList.of());
        }
    }

    private static SessionInfo createSessionInfo(Session session)
    {
        return new SessionInfo(
                session.getIdentity().getUser(),
                session.getIdentity().getPrincipal(),
                session.getIdentity().getExtraCredentials(),
                session.getCatalog(),
                session.getSchema(),
                session.getSource(),
                session.getClientInfo(),
                session.getClientTags(),
                Optional.of(session.getTimeZoneKey().getId()),
                Optional.empty(),
                session.getSystemProperties(),
                session.getConnectorProperties().entrySet().stream()
                        .collect(toImmutableMap(entry -> entry.getKey().getCatalogName(), Map.Entry::getValue)),
                session.getTraceToken());
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin);
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        connectorManager.createConnection(catalogName, connectorName, properties);
    }

    @Override
    public Lock getExclusiveLock()
    {
        throw new UnsupportedOperationException();
    }

    public PrestoSparkService getPrestoSparkService()
    {
        return prestoSparkService;
    }

    @Override
    public void close()
    {
        sparkContext.cancelAllJobs();

        try {
            if (lifeCycleManager != null) {
                lifeCycleManager.stop();
            }
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        if (instanceId != null) {
            instances.remove(instanceId);
        }
    }

    private static class TestingCompilerFactory
            implements TaskCompilerFactory
    {
        private final String instanceId;

        private TestingCompilerFactory(String instanceId)
        {
            this.instanceId = requireNonNull(instanceId, "instanceId is null");
        }

        @Override
        public TaskCompiler create()
        {
            return instances.get(instanceId).getPrestoSparkService().createTaskCompiler();
        }
    }

    private static Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }
}
