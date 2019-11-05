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
package com.facebook.presto.spark.presto;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.block.BlockJsonSerde;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.index.IndexHandleJacksonModule;
import com.facebook.presto.metadata.ColumnHandleJacksonModule;
import com.facebook.presto.metadata.FunctionHandleJacksonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InsertTableHandleJacksonModule;
import com.facebook.presto.metadata.OutputTableHandleJacksonModule;
import com.facebook.presto.metadata.PartitioningHandleJacksonModule;
import com.facebook.presto.metadata.SplitJacksonModule;
import com.facebook.presto.metadata.TableHandleJacksonModule;
import com.facebook.presto.metadata.TableLayoutHandleJacksonModule;
import com.facebook.presto.metadata.TransactionHandleJacksonModule;
import com.facebook.presto.spark.spi.CatalogConfiguration;
import com.facebook.presto.spark.spi.PrestoConfiguration;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.UUID;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class LocalQueryRunnerFactory
{
    private LocalQueryRunnerFactory() {}

    public static LocalQueryRunner createLocalQueryRunner(PrestoConfiguration prestoConfiguration)
    {
        LocalQueryRunner localQueryRunner = LocalQueryRunner.queryRunnerWithInitialTransaction(createSession());

        for (String plugin : prestoConfiguration.getPlugins()) {
            try {
                if (plugin.equals("com.facebook.presto.hive.HivePlugin")) {
                    File baseDir = new File("/tmp/" + UUID.randomUUID().toString().replaceAll("-", "_"));

                    HiveClientConfig hiveClientConfig = new HiveClientConfig();
                    HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig), ImmutableSet.of());
                    HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hiveClientConfig, new NoHdfsAuthentication());
                    FileHiveMetastore metastore = new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
                    metastore.createDatabase(Database.builder()
                            .setDatabaseName("test")
                            .setOwnerName("public")
                            .setOwnerType(PrincipalType.ROLE)
                            .build());

                    localQueryRunner.installPlugin(new HivePlugin("hive", Optional.of(metastore)));
                }
                else {
                    localQueryRunner.installPlugin((Plugin) Class.forName(plugin).getConstructor().newInstance());
                }
            }
            catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        for (CatalogConfiguration catalog : prestoConfiguration.getCatalogs()) {
            localQueryRunner.createCatalog(catalog.getCatalogName(), catalog.getPluginName(), catalog.getCatalogConfiguration());
        }

        return localQueryRunner;
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
    }

    public static <T> JsonCodec<T> createJsonCodec(Class<T> clazz, HandleResolver handleResolver)
    {
        TypeManager typeManager = new TypeRegistry();
        BlockEncodingSerde serde = new BlockEncodingManager(typeManager);

        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setKeySerializers(ImmutableMap.of(
                VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionSerializer()));
        provider.setKeyDeserializers(ImmutableMap.of(
                VariableReferenceExpression.class, new Serialization.VariableReferenceExpressionDeserializer(typeManager)));
        provider.setJsonSerializers(ImmutableMap.of(
                Block.class, new BlockJsonSerde.Serializer(serde)));
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(typeManager),
                Block.class, new BlockJsonSerde.Deserializer(serde)));
        provider.setModules(ImmutableSet.of(
                new TableHandleJacksonModule(handleResolver),
                new TableLayoutHandleJacksonModule(handleResolver),
                new ColumnHandleJacksonModule(handleResolver),
                new SplitJacksonModule(handleResolver),
                new OutputTableHandleJacksonModule(handleResolver),
                new InsertTableHandleJacksonModule(handleResolver),
                new IndexHandleJacksonModule(handleResolver),
                new TransactionHandleJacksonModule(handleResolver),
                new PartitioningHandleJacksonModule(handleResolver),
                new FunctionHandleJacksonModule(handleResolver)));
        JsonCodecFactory factory = new JsonCodecFactory(provider);
        return factory.jsonCodec(clazz);
    }
}
