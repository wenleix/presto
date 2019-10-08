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

import com.facebook.presto.spark.spi.PrestoConfiguration;
import com.facebook.presto.spark.spi.SparkFragmentCompiler;
import com.facebook.presto.spark.spi.SparkPluginFactory;
import com.facebook.presto.spark.spi.SparkQueryPlanner;
import com.facebook.presto.spark.spi.SparkResultDeserializer;
import org.apache.spark.SparkContext;

public class PrestoSparkPluginFactory
        implements SparkPluginFactory
{
    @Override
    public SparkQueryPlanner createPlanner(SparkContext sparkContext, PrestoConfiguration configuration, int hashPartitions)
    {
        return new PrestoSparkQueryPlanner(sparkContext, configuration, hashPartitions);
    }

    @Override
    public SparkFragmentCompiler createCompiler(PrestoConfiguration configuration)
    {
        return new PrestoSparkFragmentCompiler(configuration);
    }

    @Override
    public SparkResultDeserializer createDeserializer()
    {
        return new PrestoSparkResultDeserializer();
    }
}
