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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Node;

import java.util.Optional;
import java.util.function.ToIntFunction;

public abstract class BucketNodeMap
{
    private final ToIntFunction<Split> splitToBucket;

    public BucketNodeMap(ToIntFunction<Split> splitToBucket)
    {
        this.splitToBucket = splitToBucket;
    }

    public abstract Optional<Node> getAssignedNode(int bucketedId);

    public abstract int getBucketCount();

    public abstract void assignOrChangeBucketToNode(int bucketedId, Node node);

    public final Optional<Node> getAssignedNode(Split split)
    {
        return getAssignedNode(splitToBucket.applyAsInt(split));
    }
}
