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
package com.facebook.presto.server;

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WenleiTaskUpdateRequest
{
    private final SessionRepresentation session;
    // extraCredentials is stored separately from SessionRepresentation to avoid being leaked
    private final Map<String, String> extraCredentials;
    private final Optional<byte[]> serializedPlanFragment;
    private final List<TaskSource> sources;
    private final OutputBuffers outputIds;
    private final OptionalInt totalPartitions;
    private final Optional<TableWriteInfo> tableWriteInfo;

    @JsonCreator
    public WenleiTaskUpdateRequest(
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("extraCredentials") Map<String, String> extraCredentials,
            @JsonProperty("fragment") Optional<byte[]> serializedPlanFragment,
            @JsonProperty("sources") List<TaskSource> sources,
            @JsonProperty("outputIds") OutputBuffers outputIds,
            @JsonProperty("totalPartitions") OptionalInt totalPartitions,
            @JsonProperty("tableWriteInfo") Optional<TableWriteInfo> tableWriteInfo)
    {
        requireNonNull(session, "session is null");
        requireNonNull(extraCredentials, "credentials is null");
        requireNonNull(serializedPlanFragment, "fragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(outputIds, "outputIds is null");
        requireNonNull(totalPartitions, "totalPartitions is null");
        requireNonNull(tableWriteInfo, "tableWriteInfo is null");

        this.session = session;
        this.extraCredentials = extraCredentials;
        this.serializedPlanFragment = serializedPlanFragment;
        this.sources = ImmutableList.copyOf(sources);
        this.outputIds = outputIds;
        this.totalPartitions = totalPartitions;
        this.tableWriteInfo = tableWriteInfo;
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonProperty
    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials;
    }

    @JsonProperty
    public Optional<byte[]> getFragment()
    {
        return serializedPlanFragment;
    }

    @JsonProperty
    public List<TaskSource> getSources()
    {
        return sources;
    }

    @JsonProperty
    public OutputBuffers getOutputIds()
    {
        return outputIds;
    }

    @JsonProperty
    public OptionalInt getTotalPartitions()
    {
        return totalPartitions;
    }

    @JsonProperty
    public Optional<TableWriteInfo> getTableWriteInfo()
    {
        return tableWriteInfo;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("session", session)
                .add("extraCredentials", extraCredentials.keySet())
                .add("sources", sources)
                .add("outputIds", outputIds)
                .add("totalPartitions", totalPartitions)
                .toString();
    }
}
