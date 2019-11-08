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
import com.facebook.presto.spark.spi.ServiceFactory;
import com.facebook.presto.spark.spi.TaskCompiler;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkFiles;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class PrestoSparkLauncher
{
    private PrestoSparkLauncher() {}

    public static void main(String[] args)
    {
        SparkConf sparkConfiguration = new SparkConf()
                .setAppName("Simple Query")
                .set("spark.memory.fraction", "0.1")
                .set("spark.memory.storageFraction", "0.1")
                .set("spark.shuffle.memoryFraction", "0.1");
        SparkContext context = new SparkContext(sparkConfiguration);

//        Configuration configuration = new Configuration(
//                singletonList("com.facebook.presto.tpch.TpchPlugin"),
//                singletonList(new CatalogConfiguration("tpch", "tpch", emptyMap())));

        String prestoSparkJarLocation = args[0];
        if (!new File(prestoSparkJarLocation).exists()) {
            throw new IllegalArgumentException(format("File does not exist: %s", prestoSparkJarLocation));
        }
        ServiceFactory factory = createPluginFactory(prestoSparkJarLocation);

        context.addFile(prestoSparkJarLocation);
        String prestoSparkJarFileId = new File(prestoSparkJarLocation).getName();

//        String query = "select partkey, count(*) c from tpch.tiny.lineitem where partkey % 10 = 1 group by partkey having count(*) = 42";
//        QueryExecutionFactory planner = factory.createRddFactory(context, configuration, 4);
//        QueryExecution plan = planner.create(query, new SparkFragmentCompilerSupplier(configuration, prestoSparkJarFileId));
//
//        List<byte[]> serializedResult = plan.getRdd()
//                .map(tupple -> tupple._2)
//                .collect();
//
//        ResultDeserializer deserializer = factory.createResultsDeserializer();
//        List<List<Object>> result = serializedResult.stream()
//                .flatMap(bytes -> deserializer.deserialize(bytes, plan.getOutputTypes()).stream())
//                .collect(toList());

//        System.out.println("Results: " + result.size());
//        result.forEach(System.out::println);
    }

    private static ServiceFactory createPluginFactory(String jar)
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
                PrestoSparkLauncher.class.getClassLoader(),
                asList("org.apache.spark.", "com.facebook.presto.spark.spi.", "scala."));
        ServiceLoader<ServiceFactory> serviceLoader = ServiceLoader.load(ServiceFactory.class, prestoSparkLoader);
        return serviceLoader.iterator().next();
    }

    public static class SparkFragmentCompilerSupplier
            implements Supplier<TaskCompiler>, Serializable
    {
        private final Configuration configuration;
        private final String prestoSparkJarFileId;

        public SparkFragmentCompilerSupplier(Configuration configuration, String prestoSparkJarFileId)
        {
            this.configuration = requireNonNull(configuration, "configuration is null");
            this.prestoSparkJarFileId = requireNonNull(prestoSparkJarFileId, "prestoSparkJarFileId is null");
        }

        @Override
        public TaskCompiler get()
        {
            String file = SparkFiles.get(prestoSparkJarFileId);
            if (!new File(file).exists()) {
                throw new IllegalArgumentException(format("File does not exist: %s", file));
            }
            ServiceFactory factory = createPluginFactory(file);
            return factory.createService(configuration).createTaskCompiler();
        }
    }
}
