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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.testng.annotations.Test;

import static java.lang.String.format;

public class TestSparkQueryRunner
{
    @Test
    public void test()
    {
        int workers = 3;
        int partitions = 6;

        SparkConf sparkConfiguration = new SparkConf()
                .setMaster(format("local[%s]", workers))

                // .setMaster("spark://spark-master:7077")
                .setAppName("Simple Query");

        SparkContext sparkContext = new SparkContext(sparkConfiguration);

        SparkQueryRunner.PrestoConfiguration prestoConfiguration = new SparkQueryRunner.PrestoConfiguration(
                ImmutableList.of("com.facebook.presto.tpch.TpchPlugin"),
                ImmutableList.of(
                        new SparkQueryRunner.CatalogConfiguration("tpch", "tpch", ImmutableMap.of())));

        new SparkQueryRunner(prestoConfiguration, sparkContext, partitions)
                .run("select partkey, count(*) c from tpch.tiny.lineitem where partkey % 10 = 1 group by partkey having count(*) = 42");

        new SparkQueryRunner(prestoConfiguration, sparkContext, partitions)
                .run("SELECT lineitem.orderkey, lineitem.linenumber, orders.orderstatus\n" +
                        "FROM tpch.tiny.lineitem JOIN tpch.tiny.orders\n" +
                        "    ON lineitem.orderkey = orders.orderkey\n" +
                        "   WHERE lineitem.orderkey % 223 = 42 AND lineitem.linenumber = 4 and orders.orderstatus = 'O'");

        sparkContext.stop();
    }
}
