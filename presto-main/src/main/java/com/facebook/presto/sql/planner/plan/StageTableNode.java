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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.metadata.NewTableLayout;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.Symbol;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

// only used during planning -- will not be serialized
// will be replaced into TableWriteNode + TableFinishNode + TableScanNoe at the end of plan
@Immutable
public class StageTableNode
        extends PlanNode
{
    private final PlanNode source;
    // TODO: Add things like catalog/schema name
    // For now, stage table will always do identity map for simplification.

    private final String tableNameHint;
    NewTableLayout stageTableLayout;
    private final List<Symbol> inputSymbols;
    private final List<Symbol> outputSymbols; // This is used to rename symbols, but do we need this??

    public StageTableNode(
            PlanNodeId id,
            PlanNode source,
            String tableNameHint,
            NewTableLayout stageTableLayout,
            List<Symbol> inputSymbols,
            List<Symbol> outputSymbols)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.tableNameHint = requireNonNull(tableNameHint, "tableNameHint is null");
        this.stageTableLayout = requireNonNull(stageTableLayout, "stageTableLayout is null");
        this.inputSymbols = ImmutableList.copyOf(requireNonNull(inputSymbols, "inputSymbols is null"));
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputSymbols, "outputSymbols is null"));
    }

    public PlanNode getSource()
    {
        return source;
    }

    public String getTableNameHint()
    {
        return tableNameHint;
    }

    public NewTableLayout getStageTableLayout()
    {
        return stageTableLayout;
    }

    // That's the actual meat we want...

    // Partitioning can be created from the PartitioningHandle + Partitioned Column symbols. not difficult.
    // The key is still the PartitioningHandle, which need to be provided by Connector :)
    public Partitioning getTablePartitioning()
    {
        List<Symbol> tablePartitioningSymbols = outputSymbols.stream()
                .filter(symbol -> symbol.getName().startsWith("custkey"))   // HACK!!!
                .collect(toImmutableList());
        checkState(tablePartitioningSymbols.size() == 1, "Otherwise the hack failed!!!!");

        return Partitioning.create(stageTableLayout.getPartitioning(), tablePartitioningSymbols);
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitStageTable(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new StageTableNode(getId(), Iterables.getOnlyElement(newChildren), tableNameHint, stageTableLayout, inputSymbols, outputSymbols);
    }

    public List<Symbol> getInputSymbols()
    {
        return inputSymbols;
    }
}
