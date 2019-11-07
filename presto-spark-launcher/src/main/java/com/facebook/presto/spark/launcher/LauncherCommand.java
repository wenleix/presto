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
package com.facebook.presto.spark.launcher;

import com.facebook.presto.spark.spi.Configuration;
import com.facebook.presto.spark.spi.QueryExecution;
import com.facebook.presto.spark.spi.QueryExecutionFactory;
import com.facebook.presto.spark.spi.Service;
import com.facebook.presto.spark.spi.ServiceFactory;
import com.facebook.presto.spark.spi.SessionInfo;
import com.facebook.presto.spark.spi.TaskCompiler;
import com.facebook.presto.spark.spi.TaskCompilerFactory;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.hash.Hashing.combineOrdered;
import static com.google.common.io.Files.asByteSource;
import static com.google.common.io.Files.asCharSource;
import static com.google.common.io.Files.createTempDir;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Arrays.sort;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Command(name = "presto-spark-launcher", description = "Presto on Spark launcher")
public class LauncherCommand
{
    private static final Logger log = Logger.getLogger(LauncherCommand.class.getName());

    @Inject
    public HelpOption helpOption;

    @Inject
    public VersionOption versionOption = new VersionOption();

    @Inject
    public ClientOptions clientOptions = new ClientOptions();

    public void run()
    {
        String query = readFileUtf8(checkFile(new File(clientOptions.file)));

        PrestoSparkDistribution distribution = createDistribution(clientOptions);

        SparkContext sparkContext = createSparkContext(clientOptions);

        distribution.deploy(sparkContext);

        Service service = createService(distribution);
        QueryExecutionFactory executionFactory = service.createQueryExecutionFactory();
        SessionInfo sessionInfo = createSessionInfo(clientOptions);
        QueryExecution queryExecution = executionFactory.create(sparkContext, sessionInfo, query, new DistributionTaskCompilerFactory(distribution));

        List<List<Object>> results = queryExecution.execute();

        System.out.println("Rows: " + results.size());
        results.forEach(System.out::println);
    }

    private static PrestoSparkDistribution createDistribution(ClientOptions clientOptions)
    {
        return new PrestoSparkDistribution(
                new File(clientOptions.packagePath),
                new File(clientOptions.config),
                new File(clientOptions.catalogs));
    }

    private static SparkContext createSparkContext(ClientOptions clientOptions)
    {
        SparkConf sparkConfiguration = new SparkConf()
                .setAppName("Presto query: <initializing>");
        return new SparkContext(sparkConfiguration);
    }

    private static SessionInfo createSessionInfo(ClientOptions clientOptions)
    {
        // TODO:
        return new SessionInfo(
                "test",
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private static Service createService(PrestoSparkDistribution distribution)
    {
        ServiceFactory serviceFactory = createServiceFactory(new File(distribution.getLocalPackageDirectory(), "lib"));
        return serviceFactory.createService(createConfiguration(distribution));
    }

    private static ServiceFactory createServiceFactory(File directory)
    {
        checkDirectory(directory);
        List<URL> urls = new ArrayList<>();
        File[] files = directory.listFiles();
        if (files != null) {
            sort(files);
        }
        for (File file : files) {
            try {
                urls.add(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                throw new UncheckedIOException(e);
            }
        }
        PrestoSparkLoader prestoSparkLoader = new PrestoSparkLoader(
                urls,
                PrestoSparkLauncher.class.getClassLoader(),
                asList("org.apache.spark.", "com.facebook.presto.spark.spi.", "scala."));
        ServiceLoader<ServiceFactory> serviceLoader = ServiceLoader.load(ServiceFactory.class, prestoSparkLoader);
        return serviceLoader.iterator().next();
    }

    private static Configuration createConfiguration(PrestoSparkDistribution distribution)
    {
        // TODO
        return new Configuration(
                distribution.getLocalConfigFile().getAbsolutePath(),
                new File(distribution.getLocalPackageDirectory(), "plugin").getAbsolutePath(),
                distribution.getLocalCatalogsDirectory().getAbsolutePath(),
                ImmutableMap.of());
    }

    private static File checkFile(File file)
    {
        checkArgument(file.exists() && file.isFile(), "file does not exist: %s", file);
        checkArgument(file.canRead(), "file is not readable: %s", file);
        return file;
    }

    private static File checkDirectory(File directory)
    {
        checkArgument(directory.exists() && directory.isDirectory(), "directory does not exist: %s", directory);
        checkArgument(directory.canRead() && directory.canExecute(), "directory is not readable: %s", directory);
        return directory;
    }

    private static String readFileUtf8(File file)
    {
        try {
            return asCharSource(file, UTF_8).read();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class PrestoSparkDistribution
            implements Serializable
    {
        private static final String CATALOGS_ARCHIVE_NAME = "catalogs.tar.gz";

        private final File packageFile;
        private final File config;
        private final File catalogs;
        private final String fingerprint;

        public PrestoSparkDistribution(File packageFile, File config, File catalogs)
        {
            this.packageFile = checkFile(requireNonNull(packageFile, "packageFile is null"));
            checkFile(config);
            checkArgument(!config.getName().equals(packageFile.getName()), "package file name and config file name must be different: %s", config.getName());
            this.config = requireNonNull(config, "config is null");
            checkDirectory(catalogs);
            checkArgument(!catalogs.getName().equals(packageFile.getName()), "catalogs directory name and package file name must be different: %s", catalogs.getName());
            checkArgument(!catalogs.getName().equals(config.getName()), "catalogs directory name and config file name must be different: %s", catalogs.getName());
            this.catalogs = requireNonNull(catalogs, "catalogs is null");
            this.fingerprint = computeFingerprint(packageFile, config, catalogs);
        }

        private static String computeFingerprint(File packageFile, File config, File catalogs)
        {
            try {
                ImmutableList.Builder<HashCode> hashes = ImmutableList.builder();
                hashes.add(asByteSource(packageFile).hash(Hashing.sha256()));
                hashes.add(asByteSource(config).hash(Hashing.sha256()));
                File[] catalogConfigurations = catalogs.listFiles();
                sort(catalogConfigurations);
                for (File catalogConfiguration : catalogConfigurations) {
                    hashes.add(Hashing.sha256().hashString(catalogConfiguration.getName(), UTF_8));
                    hashes.add(asByteSource(catalogConfiguration).hash(Hashing.sha256()));
                }
                return combineOrdered(hashes.build()).toString();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void deploy(SparkContext context)
        {
            context.addFile(packageFile.getAbsolutePath());
            context.addFile(config.getAbsolutePath());

            File tempDir = createTempDir();
            tempDir.deleteOnExit();
            File catalogsArchive = new File(tempDir, CATALOGS_ARCHIVE_NAME);
            TarGz.create(catalogs, catalogsArchive);
            context.addFile(catalogsArchive.getAbsolutePath());
        }

        public File getLocalPackageDirectory()
        {
            return ensureDecompressed(getLocalFile(packageFile.getName()), new File(SparkFiles.getRootDirectory()));
        }

        public File getLocalConfigFile()
        {
            return getLocalFile(config.getName());
        }

        public File getLocalCatalogsDirectory()
        {
            return ensureDecompressed(getLocalFile(CATALOGS_ARCHIVE_NAME), new File(SparkFiles.getRootDirectory()));
        }

        public String getFingerprint()
        {
            return fingerprint;
        }

        private static File ensureDecompressed(File archive, File outputDirectory)
        {
            String packageDirectoryName = TarGz.getRootDirectoryName(archive);
            File packageDirectory = new File(outputDirectory, packageDirectoryName);
            log.info(format("Package directory: %s", packageDirectory));
            if (packageDirectory.exists()) {
                verify(packageDirectory.isDirectory(), "package directory is not a directory: %s", packageDirectory);
                log.info(format("Skipping decompression step as package is already decompressed: %s", packageDirectory));
                return packageDirectory;
            }
            Stopwatch stopwatch = Stopwatch.createStarted();
            log.info(format("Decompressing: %s", packageDirectory));
            TarGz.extract(archive, outputDirectory);
            log.info(format("Decompression took: %sms", stopwatch.elapsed(MILLISECONDS)));
            return packageDirectory;
        }

        private static File getLocalFile(String name)
        {
            String path = requireNonNull(SparkFiles.get(name), "path is null");
            return checkFile(new File(path));
        }
    }

    public static class DistributionTaskCompilerFactory
            implements TaskCompilerFactory
    {
        private static final Cache<String, Service> services = CacheBuilder.newBuilder().build();

        private final PrestoSparkDistribution distribution;

        public DistributionTaskCompilerFactory(PrestoSparkDistribution distribution)
        {
            this.distribution = requireNonNull(distribution, "distribution is null");
        }

        @Override
        public TaskCompiler create()
        {
            Service service;
            try {
                service = services.get(distribution.getFingerprint(), () -> createService(distribution));
            }
            catch (ExecutionException e) {
                throw new UncheckedExecutionException(e);
            }
            return service.createTaskCompiler();
        }
    }
}
