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
package com.facebook.presto.spark.runner;

import com.facebook.presto.spark.spi.CatalogConfiguration;
import com.facebook.presto.spark.spi.PrestoConfiguration;
import com.facebook.presto.spark.spi.SparkFragmentCompiler;
import com.facebook.presto.spark.spi.SparkPlan;
import com.facebook.presto.spark.spi.SparkPluginFactory;
import com.facebook.presto.spark.spi.SparkQueryPlanner;
import com.facebook.presto.spark.spi.SparkResultDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PrestoSparkRunner
{
    private PrestoSparkRunner() {}

    public static void main(String[] args)
    {
        SparkConf sparkConfiguration = new SparkConf()
                .setAppName("Simple Query")
                .set("spark.memory.fraction", "0.1")
                .set("spark.memory.storageFraction", "0.1")
                .set("spark.shuffle.memoryFraction", "0.1");
        SparkContext context = new SparkContext(sparkConfiguration);

        PrestoConfiguration configuration = new PrestoConfiguration(
                singletonList("com.facebook.presto.tpch.TpchPlugin"),
                singletonList(new CatalogConfiguration("tpch", "tpch", emptyMap())));

        String prestoSparkJarLocation = args[0];
        if (!new File(prestoSparkJarLocation).exists()) {
            throw new IllegalArgumentException(format("File does not exist: %s", prestoSparkJarLocation));
        }
        SparkPluginFactory factory = createPluginFactory(prestoSparkJarLocation);

        context.addFile(prestoSparkJarLocation);
        String prestoSparkJarFileId = new File(prestoSparkJarLocation).getName();

        String query = "select partkey, count(*) c from tpch.tiny.lineitem where partkey % 10 = 1 group by partkey having count(*) = 42";
        SparkQueryPlanner planner = factory.createPlanner(context, configuration, 4);
        SparkPlan plan = planner.plan(query, new SparkFragmentCompilerSupplier(configuration, prestoSparkJarFileId));

        List<byte[]> serializedResult = plan.getRdd()
                .map(tupple -> tupple._2)
                .collect();

        SparkResultDeserializer deserializer = factory.createDeserializer();
        List<List<Object>> result = serializedResult.stream()
                .flatMap(bytes -> deserializer.deserialize(bytes, plan.getOutputTypes()).stream())
                .collect(toList());

        System.out.println("Results: " + result.size());
        result.forEach(System.out::println);
    }

    private static SparkPluginFactory createPluginFactory(String jar)
    {
        URL url;
        try {
            url = new File(jar).toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
        PrestoSparkLoader prestoSparkLoader = new PrestoSparkLoader(
                singletonList(url),
                PrestoSparkRunner.class.getClassLoader(),
                asList("org.apache.spark.", "com.facebook.presto.spark.spi.", "scala."));
        ServiceLoader<SparkPluginFactory> serviceLoader = ServiceLoader.load(SparkPluginFactory.class, prestoSparkLoader);
        return serviceLoader.iterator().next();
    }

    public static class SparkFragmentCompilerSupplier
            implements Supplier<SparkFragmentCompiler>, Serializable
    {
        private final PrestoConfiguration configuration;
        private final String prestoSparkJarFileId;

        public SparkFragmentCompilerSupplier(PrestoConfiguration configuration, String prestoSparkJarFileId)
        {
            this.configuration = requireNonNull(configuration, "configuration is null");
            this.prestoSparkJarFileId = requireNonNull(prestoSparkJarFileId, "prestoSparkJarFileId is null");
        }

        @Override
        public SparkFragmentCompiler get()
        {
            String file = SparkFiles.get(prestoSparkJarFileId);
            if (!new File(file).exists()) {
                throw new IllegalArgumentException(format("File does not exist: %s", file));
            }
            SparkPluginFactory factory = createPluginFactory(file);
            return factory.createCompiler(configuration);
        }
    }
}
