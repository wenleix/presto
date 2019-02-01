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

import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;

import javax.annotation.concurrent.Immutable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@Immutable
public class SubPlan
{
    private final PlanFragment fragment;
    private final List<SubPlan> dataDependencies;
    private final List<Reference<SubPlan>> dependencies;

    public SubPlan(PlanFragment fragment, List<SubPlan> dataDependencies, List<Reference<SubPlan>> dependencies)
    {
        requireNonNull(fragment, "fragment is null");
        requireNonNull(dataDependencies, "inputs is null");
        requireNonNull(dependencies, "dependencies is null");

        this.fragment = fragment;
        this.dataDependencies = ImmutableList.copyOf(dataDependencies);
        this.dependencies = ImmutableList.copyOf(dependencies);
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public List<SubPlan> getDataDependencies()
    {
        return dataDependencies;
    }

    public List<Reference<SubPlan>> getExecutionDependencies()
    {
        return dependencies;
    }

    public SubPlan withExecutionDependencies(List<Reference<SubPlan>> dependencies)
    {
        Set<PlanFragmentId> dependencyIds = dependencies.stream()
                .map(Reference::get)
                .map(SubPlan::getFragment)
                .map(PlanFragment::getId)
                .collect(toImmutableSet());
        return new SubPlan(fragment.withExecutionDependencies(dependencyIds), dataDependencies, dependencies);
    }

    /**
     * Flattens the subplan and returns all PlanFragments in the tree
     */
    public List<PlanFragment> getAllFragments()
    {
        Map<PlanFragmentId, PlanFragment> fragments = new HashMap<>();
        fragments.put(fragment.getId(), fragment);
        for (SubPlan dataDependency : dataDependencies) {
            fragments.put(dataDependency.getFragment().getId(), dataDependency.getFragment());
        }
        for (Reference<SubPlan> dependency : dependencies) {
            fragments.put(dependency.get().getFragment().getId(), dependency.get().getFragment());
        }
        return ImmutableList.copyOf(fragments.values());
    }

    public void sanityCheck()
    {
        Multiset<PlanFragmentId> exchangeIds = fragment.getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableMultiset());

        Multiset<PlanFragmentId> inputIds = dataDependencies.stream()
                .map(SubPlan::getFragment)
                .map(PlanFragment::getId)
                .collect(toImmutableMultiset());

        checkState(exchangeIds.equals(inputIds), "Subplan exchange ids don't match input fragment ids (%s vs %s)", exchangeIds, inputIds);

        Set<PlanFragmentId> dependencyIds = dependencies.stream()
                .map(Reference::get)
                .map(SubPlan::getFragment)
                .map(PlanFragment::getId)
                .collect(toImmutableSet());

        checkState(fragment.getExecutionDependencies().equals(dependencyIds), "Subplan dependency ids don't match fragment dependency ids (%s vs %s)", dependencyIds, fragment.getExecutionDependencies());

        for (SubPlan child : dataDependencies) {
            child.sanityCheck();
        }

        for (Reference<SubPlan> dependency : dependencies) {
            dependency.get().sanityCheck();
        }
    }
}