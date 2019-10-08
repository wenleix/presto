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

import com.facebook.presto.spark.spi.CatalogConfiguration;
import com.facebook.presto.spark.spi.PrestoConfiguration;
import com.facebook.presto.spark.spi.SparkFragmentCompiler;
import com.facebook.presto.spark.spi.SparkPlan;
import com.facebook.presto.spark.spi.SparkQueryPlanner;
import com.facebook.presto.spark.spi.SparkResultDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.List;
import java.util.function.Supplier;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class TestSparkQueryRunner
{
    @Test
    public void test()
    {
        int workers = 3;
        int partitions = 6;

        SparkConf sparkConfiguration = new SparkConf()
                .setMaster(format("local[%s]", workers))
                .setAppName("Simple Query");

        SparkContext sparkContext = new SparkContext(sparkConfiguration);

        PrestoConfiguration prestoConfiguration = new PrestoConfiguration(
                ImmutableList.of("com.facebook.presto.tpch.TpchPlugin"),
                ImmutableList.of(
                        new CatalogConfiguration("tpch", "tpch", ImmutableMap.of())));

        runQueryOnSpark(
                "select partkey, count(*) c from tpch.tiny.lineitem where partkey % 10 = 1 group by partkey having count(*) = 42",
                prestoConfiguration,
                sparkContext,
                partitions);

        runQueryOnSpark(
                "SELECT lineitem.orderkey, lineitem.linenumber, orders.orderstatus\n" +
                        "FROM tpch.tiny.lineitem JOIN tpch.tiny.orders\n" +
                        "    ON lineitem.orderkey = orders.orderkey\n" +
                        "   WHERE lineitem.orderkey % 223 = 42 AND lineitem.linenumber = 4 and orders.orderstatus = 'O'",
                prestoConfiguration,
                sparkContext,
                partitions);

        sparkContext.stop();
    }

    public static void runQueryOnSpark(String query, PrestoConfiguration configuration, SparkContext sparkContext, int partitions)
    {
        PrestoSparkPluginFactory factory = new PrestoSparkPluginFactory();
        SparkQueryPlanner planner = factory.createPlanner(sparkContext, configuration, partitions);
        SparkPlan plan = planner.plan(query, new SparkFragmentCompilerSupplier(configuration));

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

    public static class SparkFragmentCompilerSupplier
            implements Supplier<SparkFragmentCompiler>, Serializable
    {
        private final PrestoConfiguration configuration;

        public SparkFragmentCompilerSupplier(PrestoConfiguration configuration)
        {
            this.configuration = configuration;
        }

        @Override
        public SparkFragmentCompiler get()
        {
            PrestoSparkPluginFactory factory = new PrestoSparkPluginFactory();
            return factory.createCompiler(configuration);
        }
    }
}
