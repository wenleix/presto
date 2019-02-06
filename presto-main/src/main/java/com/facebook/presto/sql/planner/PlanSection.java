package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PlanSection
{
    private final PlanNode root;
    // execution dependency
    private final List<Reference<PlanSection>> dependencies = new ArrayList<>();

    public PlanFragment rootFragment;
    public Map<PlanNode, PlanSection> scanDependentSection = new HashMap<>();

    public PlanSection(PlanNode root)
    {
        this.root = requireNonNull(root, "root is null");
    }

    public PlanNode getPlanRoot()
    {
        return root;
    }

    public void addExecutionDependency(PlanSection parentSection)
    {
        dependencies.add(Reference.of(parentSection));
    }

    public List<Reference<PlanSection>> getDependencies()
    {
        return dependencies;
    }
}
