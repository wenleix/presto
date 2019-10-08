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
package com.facebook.presto.spark.spi;

import org.apache.spark.api.java.JavaPairRDD;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SparkPlan
{
    private final JavaPairRDD<Integer, byte[]> rdd;
    private final List<Object> outputTypes;

    public SparkPlan(JavaPairRDD<Integer, byte[]> rdd, List<Object> outputTypes)
    {
        this.rdd = requireNonNull(rdd, "rdd is null");
        this.outputTypes = requireNonNull(outputTypes, "outputTypes is null");
    }

    public JavaPairRDD<Integer, byte[]> getRdd()
    {
        return rdd;
    }

    public List<Object> getOutputTypes()
    {
        return outputTypes;
    }
}
