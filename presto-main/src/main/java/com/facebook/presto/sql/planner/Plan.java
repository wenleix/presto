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
package com.facebook.presto.sql.planner;

import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class Plan
{
    private final PlanSection rootSection;
    private final TypeProvider types;
    private final StatsAndCosts statsAndCosts;

    public Plan(PlanSection rootSection, TypeProvider types, StatsAndCosts statsAndCosts)
    {
        this.rootSection = requireNonNull(rootSection, "rootStage is null");
        this.types = requireNonNull(types, "types is null");
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
    }

    public PlanSection getRootSection()
    {
        return rootSection;
    }

    public List<PlanSection> getSections()
    {
        Set<Reference<PlanSection>> references = new HashSet<>();
        LinkedList<Reference<PlanSection>> queue = new LinkedList<>();
        queue.add(new Reference<>(rootSection));
        while (!queue.isEmpty()) {
            Reference<PlanSection> reference = queue.poll();
            references.add(reference);
            for (Reference<PlanSection> dependency : reference.get().getDependencies()) {
                if (!references.contains(dependency)) {
                    queue.add(dependency);
                }
            }
        }
        return references.stream()
                .map(Reference::get)
                .collect(toImmutableList());
    }

    public TypeProvider getTypes()
    {
        return types;
    }

    public StatsAndCosts getStatsAndCosts()
    {
        return statsAndCosts;
    }
}
