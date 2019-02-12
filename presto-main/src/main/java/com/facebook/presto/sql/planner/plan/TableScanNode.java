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

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.PlanSection;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Immutable
public class TableScanNode
        extends PlanNode
{
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column

    // fields below will not be serialized
    // See also: https://github.com/prestodb/presto/pull/12325

    private final TableHandle table;
    private final TableLayoutHandleProvider tableLayout;

    // Used during predicate refinement over multiple passes of predicate pushdown
    // TODO: think about how to get rid of this in new planner
    private final TupleDomain<ColumnHandle> currentConstraint;

    private final TupleDomain<ColumnHandle> enforcedConstraint;

    // Wenlei!@#$%
    // It shoudn't be considered as a source, but should be done in some "PlanSectioner"
    private Optional<TableFinishNode> stagedTableFinishedNode;

    // Hack
    public PlanSection section;

    @JsonCreator
    public TableScanNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("outputSymbols") List<Symbol> outputs,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments)
    {
        // This constructor is for JSON deserialization only. Do not use.
        super(id);
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs), "assignments does not cover all of outputs");

        this.table = null;
        this.tableLayout = TableLayoutHandleProvider.empty();
        this.currentConstraint = null;
        this.enforcedConstraint = null;
        this.stagedTableFinishedNode = Optional.empty();
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments)
    {
        this(
                id,
                table,
                outputs,
                assignments,
                TableLayoutHandleProvider.empty(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            TableLayoutHandleProvider tableLayout,
            TupleDomain<ColumnHandle> currentConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint)
    {
        this(id, table, outputs, assignments, TableLayoutHandleProvider.empty(), currentConstraint, enforcedConstraint, Optional.empty());
    }

    public TableScanNode(
            PlanNodeId id,
            TableHandle table,
            List<Symbol> outputs,
            Map<Symbol, ColumnHandle> assignments,
            TableLayoutHandleProvider tableLayout,
            TupleDomain<ColumnHandle> currentConstraint,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<TableFinishNode> stagedTableFinishedNode)
    {
        super(id);
        this.table = table;
        this.outputSymbols = ImmutableList.copyOf(requireNonNull(outputs, "outputs is null"));
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        checkArgument(assignments.keySet().containsAll(outputs), "assignments does not cover all of outputs");
        this.tableLayout = requireNonNull(tableLayout, "tableLayout is null");
        this.currentConstraint = requireNonNull(currentConstraint, "currentConstraint is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        if (!currentConstraint.isAll() || !enforcedConstraint.isAll()) {
            tableLayout.get();
        }

        this.stagedTableFinishedNode = requireNonNull(stagedTableFinishedNode);
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @JsonProperty("sourceFragmentIds")
    public List<PlanFragmentId> getSourceFragmentIds()
    {
        if (section == null) {
            return ImmutableList.of();
        }

        PlanFragmentId sourceId = section.scanDependentSection.get(this).rootFragment.getId();

        if (sourceId != null) {
            return ImmutableList.of(sourceId);
        }
        else {
            return ImmutableList.of();
        }
    }

    public TableHandle getTable()
    {
        return table;
    }

    public TableLayoutHandleProvider getLayout()
    {
        return tableLayout;
    }

    /**
     * A TupleDomain that represents a predicate that every row this TableScan node
     * produces is guaranteed to satisfy.
     * <p>
     * This guarantee can have different origins.
     * For example, it may be successful predicate push down, or inherent guarantee provided by the underlying data.
     */
    public TupleDomain<ColumnHandle> getCurrentConstraint()
    {
        // currentConstraint can be pretty complex. As a result, it may incur a significant cost to serialize, store, and transport.
        checkState(currentConstraint != null, "currentConstraint should only be used in planner. It is not transported to workers.");
        return currentConstraint;
    }

    /**
     * A TupleDomain that represents a predicate that has been successfully pushed into
     * this TableScan node. In other words, predicates that were removed from filters
     * above the TableScan node because the TableScan node can guarantee it.
     * <p>
     * This field is used to make sure that predicates which were previously pushed down
     * do not get lost in subsequent refinements of the table layout.
     */
    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        // enforcedConstraint can be pretty complex. As a result, it may incur a significant cost to serialize, store, and transport.
        checkState(enforcedConstraint != null, "enforcedConstraint should only be used in planner. It is not transported to workers.");
        return enforcedConstraint;
    }

    @Override
    public List<PlanNode> getSources()
    {
        if (stagedTableFinishedNode.isPresent()) {
            return ImmutableList.of(stagedTableFinishedNode.get());
        }
        else {
            return ImmutableList.of();
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("tableLayout", tableLayout)
                .add("outputSymbols", outputSymbols)
                .add("assignments", assignments)
                .add("currentConstraint", currentConstraint)
                .add("enforcedConstraint", enforcedConstraint)
                .toString();
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.isEmpty(), "newChildren is not empty");
        return this;
    }

    public TableScanNode withStagedTableFinishNode(TableFinishNode tableFinishNode)
    {
        return new TableScanNode(
                getId(),
                table,
                outputSymbols,
                assignments,
                tableLayout,
                currentConstraint,
                enforcedConstraint,
                Optional.of(tableFinishNode));
    }

    // be called after sectionize finished
    public void deleteDependency()
    {
        this.stagedTableFinishedNode = Optional.empty();
    }

    // TODO: Change all the Optional<TableLayoutHandle> to TableLayoutHandleProvider
    public static class TableLayoutHandleProvider
    {
        private TableLayoutHandle value;
        private final Supplier<TableLayoutHandle> supplier;

        private TableLayoutHandleProvider(TableLayoutHandle value, Supplier<TableLayoutHandle> supplier)
        {
            checkArgument(!(value != null && supplier != null));
            this.value = value;
            this.supplier = supplier;
        }

        public synchronized boolean isEmpty()
        {
            return value == null && supplier == null;
        }

        public synchronized boolean isLoaded()
        {
            return value != null;
        }

        public synchronized TableLayoutHandle getOrCalculate()
        {
            assureLoaded();
            return this.value;
        }

        public synchronized void assureLoaded()
        {
            if (value == null) {
                return;
            }
            value = supplier.get();
        }

        public Optional<TableLayoutHandle> toOptional()
        {
            if (isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(getOrCalculate());
        }

        public static TableLayoutHandleProvider empty()
        {
            return new TableLayoutHandleProvider(null, null);
        }

        public static TableLayoutHandleProvider of(TableLayoutHandle value)
        {
            return new TableLayoutHandleProvider(value, null);
        }

        public static TableLayoutHandleProvider of(Supplier<TableLayoutHandle> value)
        {
            return new TableLayoutHandleProvider(null, value);
        }
    }
}
