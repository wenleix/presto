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
package com.facebook.presto.spark.testing;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import static com.facebook.presto.spark.testing.Processes.destroyProcess;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static java.lang.String.format;

/**
 * IMPORTANT!
 * <p>
 * Before running this test from an IDE, the project must be built with maven.
 * <p>
 * Please run:
 * <p>
 * ./mvnw clean install -pl presto-spark-launcher,presto-spark-package -am -DskipTests
 * <p>
 * from the project root after making any changes to the presto-spark-* codebase,
 * otherwise this test may be running an old code version
 */
public class TestPrestoSparkLauncher
{
    private static final Logger log = Logger.get(TestPrestoSparkLauncher.class);

    private File tempDir;

    private DockerCompose dockerCompose;
    private Process composeProcess;
    private LocalQueryRunner queryRunner;

    private File prestoLauncher;
    private File prestoPackage;

    @BeforeClass
    public void setUp()
            throws Exception
    {
//        tempDir = createTempDir();
//        tempDir.deleteOnExit();
//
//        File composeYaml = extractResource("docker-compose.yml", tempDir);
//        dockerCompose = new DockerCompose(composeYaml);
//        dockerCompose.verifyInstallation();
//        dockerCompose.pull();
//        composeProcess = dockerCompose.up(ImmutableMap.of(
//                "spark-master", 1,
//                "spark-worker", 3,
//                "hadoop-master", 1));
//        ensureProcessIsRunning(composeProcess, new Duration(10, SECONDS));
//
//        String hiveContainerAddress = dockerCompose.getContainerAddress("hadoop-master");
//
//        Session session = testSessionBuilder()
//                .setCatalog("hive")
//                .setSchema("default")
//                .build();
//        queryRunner = new LocalQueryRunner(session);
//        HiveHadoop2Plugin plugin = new HiveHadoop2Plugin();
//        ConnectorFactory hiveConnectorFactory = getOnlyElement(plugin.getConnectorFactories());
//        addStaticResolution("hadoop-master", hiveContainerAddress);
//        queryRunner.createCatalog(
//                "hive",
//                hiveConnectorFactory,
//                ImmutableMap.of(
//                        "hive.metastore.uri", format("thrift://%s:9083", hiveContainerAddress)));
//        queryRunner.createCatalog("tpch", new TpchConnectorFactory(), ImmutableMap.of());

        File projectRoot = resolveProjectRoot();
        prestoLauncher = resolveFile(new File(projectRoot, "presto-spark-launcher/target"), Pattern.compile("presto-spark-launcher-.+-shaded\\.jar"));
        logPackageInfo(prestoLauncher);
        prestoPackage = resolveFile(new File(projectRoot, "presto-spark-package/target"), Pattern.compile("presto-spark-package-.+\\.tar\\.gz"));
        logPackageInfo(prestoPackage);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (composeProcess != null) {
            destroyProcess(composeProcess);
            composeProcess = null;
        }
        if (dockerCompose != null) {
            dockerCompose.down();
            dockerCompose = null;
        }
        if (queryRunner != null) {
            queryRunner.close();
            queryRunner = null;
        }
        if (tempDir != null) {
            deleteRecursively(tempDir.toPath(), ALLOW_INSECURE);
            tempDir = null;
        }
    }

//    private static int runDockerCompose(File composeYaml, List<String> arguments)
//    {
//        return waitForProcess(startDockerCompose(composeYaml, arguments));
//    }

//    private static Process startDockerCompose(File composeYaml, List<String> arguments)
//    {
//        return;
//    }

    private static File extractResource(String resource, File destinationDirectory)
    {
        File file = new File(destinationDirectory, Paths.get(resource).getFileName().toString());
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(toByteArray(getResource(resource)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }

    private static File resolveProjectRoot()
    {
        File directory = new File(System.getProperty("user.dir"));
        while (true) {
            File prestoSparkTestingDirectory = new File(directory, "presto-spark-testing");
            if (prestoSparkTestingDirectory.exists() && prestoSparkTestingDirectory.isDirectory()) {
                return directory;
            }
            directory = directory.getParentFile();
            if (directory == null) {
                throw new IllegalStateException("working directory must be set to a directory within the presto project");
            }
        }
    }

    private static File resolveFile(File directory, Pattern pattern)
            throws FileNotFoundException
    {
        checkArgument(directory.exists() && directory.isDirectory(), "directory does not exist: %s", directory);
        List<File> result = new ArrayList<>();
        for (File file : directory.listFiles()) {
            if (pattern.matcher(file.getName()).matches()) {
                result.add(file);
            }
        }
        if (result.isEmpty()) {
            throw new FileNotFoundException(format("directory %s doesn't contain a file that matches the given pattern: %s", directory, pattern));
        }
        if (result.size() > 1) {
            throw new FileNotFoundException(format("directory %s contains multiple files that match the given pattern: %s", directory, pattern));
        }
        return getOnlyElement(result);
    }

    private static void logPackageInfo(File file)
            throws IOException
    {
        long lastModified = file.lastModified();
        log.info(
                "%s size: %s modified: %s sha256sum: %s",
                file,
                file.length(),
                new Date(lastModified),
                Files.asByteSource(file).hash(Hashing.sha256()).toString());
        long minutesSinceLastModified = (System.currentTimeMillis() - lastModified) / 1000 / 60;
        if (minutesSinceLastModified > 30) {
            log.warn("%s was modified more than 30 minutes ago. " +
                    "This test doesn't trigger automatic build. " +
                    "After any changes are applied - the project must be completely rebuilt for the changes to take effect.", file);
        }
    }

    @Test
    public void test()
            throws Exception
    {
        System.out.println(queryRunner.execute("SHOW SCHEMAS"));
        System.out.println(queryRunner.execute("CREATE TABLE wat AS SELECT * FROM tpch.tiny.nation"));
        System.out.println(queryRunner.execute("SELECT * FROM wat"));
    }
}
