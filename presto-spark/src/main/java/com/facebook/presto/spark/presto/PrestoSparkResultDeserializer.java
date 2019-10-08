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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeUtil;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.spark.spi.SparkResultDeserializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spark.presto.LocalQueryRunnerFactory.createSession;
import static java.util.stream.Collectors.toList;

public class PrestoSparkResultDeserializer
        implements SparkResultDeserializer
{
    private final PagesSerde pagesSerde = createPagesSerde();

    @Override
    public List<List<Object>> deserialize(byte[] results, List<Object> types)
    {
        return getPageValues(deserializePage(pagesSerde, results), types.stream().map(Type.class::cast).collect(toList()));
    }

    private static Page deserializePage(PagesSerde pagesSerde, byte[] data)
    {
        return pagesSerde.deserialize(deserializeSerializedPage(data));
    }

    private static PagesSerde createPagesSerde()
    {
        TypeManager typeManager = new TypeRegistry();
        BlockEncodingManager blockEncodingManager = new BlockEncodingManager(typeManager, ImmutableSet.of());
        return new PagesSerde(blockEncodingManager, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private static SerializedPage deserializeSerializedPage(byte[] data)
    {
        return PagesSerdeUtil.readSerializedPages(Slices.wrappedBuffer(data).getInput()).next();
    }

    private static List<List<Object>> getPageValues(Page page, List<Type> types)
    {
        ConnectorSession connectorSession = createSession().toConnectorSession();
        List<List<Object>> result = new ArrayList<>();
        for (int position = 0; position < page.getPositionCount(); position++) {
            List<Object> values = new ArrayList<>(page.getChannelCount());
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Type type = types.get(channel);
                Block block = page.getBlock(channel);

                values.add(type.getObjectValue(connectorSession, block, position));
            }
            result.add(Collections.unmodifiableList(values));
        }
        return Collections.unmodifiableList(result);
    }
}
