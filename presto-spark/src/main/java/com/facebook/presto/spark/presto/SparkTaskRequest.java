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

import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.sql.planner.PlanFragment;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

// TODO: What's a better name??? Or should we use the same TaskUpdateRequest??
// See TaskUpdateRequest
public class SparkTaskRequest
{
    // TODO: session and extraCredentials is probably also required in SparkTaskRequest
    private final PlanFragment fragment;

    private final List<TaskSource> sources;

    @JsonCreator
    public SparkTaskRequest(
            @JsonProperty("fragment") PlanFragment fragment,
            @JsonProperty("sources") List<TaskSource> sources)
    {
        this.fragment = requireNonNull(fragment);
        this.sources = ImmutableList.copyOf(requireNonNull(sources));
    }

    @JsonProperty
    public PlanFragment getFragment()
    {
        return fragment;
    }

    @JsonProperty
    public List<TaskSource> getSources()
    {
        return sources;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("fragment", fragment)
                .add("sources", sources)
                .toString();
    }
}
