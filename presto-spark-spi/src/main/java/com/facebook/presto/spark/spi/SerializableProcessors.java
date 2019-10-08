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

import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Iterator;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;

public class SerializableProcessors
{
    private SerializableProcessors() {}

    public static PairFlatMapFunction<String, Integer, byte[]> createSerializableProcessor(Supplier<SparkFragmentCompiler> compiler)
    {
        return (fragment) -> compiler.get().compileFragment(fragment, emptyMap());
    }

    public static PairFlatMapFunction<Iterator<Tuple2<Integer, byte[]>>, Integer, byte[]> createSerializableProcessor(
            Supplier<SparkFragmentCompiler> compiler,
            String fragment,
            String inputId)
    {
        return (input) -> compiler.get().compileFragment(fragment, singletonMap(inputId, input));
    }

    public static FlatMapFunction2<Iterator<Tuple2<Integer, byte[]>>, Iterator<Tuple2<Integer, byte[]>>, Tuple2<Integer, byte[]>> createSerializableProcessor(
            Supplier<SparkFragmentCompiler> compiler,
            String fragment,
            String inputId1,
            String inputId2)
    {
        return (input1, input2) -> {
            HashMap<String, Iterator<Tuple2<Integer, byte[]>>> inputsMap = new HashMap<>();
            inputsMap.put(inputId1, input1);
            inputsMap.put(inputId2, input2);
            return compiler.get().compileFragment(fragment, unmodifiableMap(inputsMap));
        };
    }
}
