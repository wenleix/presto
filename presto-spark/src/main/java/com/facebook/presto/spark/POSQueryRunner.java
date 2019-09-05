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

import com.facebook.presto.Session;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import java.io.File;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public final class POSQueryRunner
{
    private POSQueryRunner() {}

    public static POSLocalQueryRunner createLocalQueryRunner()
    {
        File tempDir = Files.createTempDir();

        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tpch")
                .build();

        POSLocalQueryRunner posLocalQueryRunner = new POSLocalQueryRunner(session);

        // add tpch
        posLocalQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        // add hive
        File hiveDir = new File(tempDir, "hive_data");
//        ExtendedHiveMetastore metastore = createTestingFileHiveMetastore(hiveDir);
//        metastore.createDatabase(Database.builder()
//                .setDatabaseName("tpch")
//                .setOwnerName("public")
//                .setOwnerType(PrincipalType.ROLE)
//                .build());

//        HiveConnectorFactory hiveConnectorFactory = new HiveConnectorFactory(
//                "hive",
//                POSQueryRunner.class.getClassLoader(),
//                Optional.of(metastore));

//        Map<String, String> hiveCatalogConfig = ImmutableMap.<String, String>builder()
//                .put("hive.max-split-size", "10GB")
//                .build();

//        localQueryRunner.createCatalog("hive", hiveConnectorFactory, hiveCatalogConfig);

//        localQueryRunner.execute("CREATE TABLE orders AS SELECT * FROM tpch.sf1.orders");
//        localQueryRunner.execute("CREATE TABLE lineitem AS SELECT * FROM tpch.sf1.lineitem");
        return posLocalQueryRunner;
    }
}
