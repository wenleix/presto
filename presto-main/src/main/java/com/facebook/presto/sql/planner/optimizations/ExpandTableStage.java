package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.ExchangeTableDescriptor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.StageTableNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.google.common.base.Verify.verify;
import static org.glassfish.jersey.internal.util.collection.ImmutableCollectors.toImmutableList;

// Expand TableStage to TableScan - TableFinish - TableWriter
public class ExpandTableStage
    implements PlanOptimizer
{
    private final Metadata metadata;

    public ExpandTableStage(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector)
    {
        return SimplePlanRewriter.rewriteWith(new Rewriter(types, session, metadata, symbolAllocator, idAllocator), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final TypeProvider types;
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Session session;
        private final Metadata metadata;

        private Rewriter(
                TypeProvider types,
                Session session,
                Metadata metadata,
                SymbolAllocator symbolAllocator,
                PlanNodeIdAllocator idAllocator)
        {
            this.types = types;
            this.session = session;
            this.metadata = metadata;
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitStageTable(StageTableNode node, RewriteContext<Void> context)
        {
            String catalogName = "hive_bucketed";

            ConnectorId connectorId = metadata.getCatalogHandle(session, catalogName)
                    .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + catalogName));

            /*
             * The old way.........

            String bucketedColumn = getOnlyElement(
                    node.getInputSymbols().stream()
                            .filter(symbol -> symbol.getName().startsWith("custkey"))
                            .map(Symbol::getName)
                            .collect(toImmutableList()));

            Map<String, Object> properties = metadata.getTablePropertyManager().getProperties(
                    connectorId,
                    catalogName,
                    ImmutableMap.of(
                            // those should be stored in StageTableNode...
                            "bucketed_by", new ArrayConstructor(ImmutableList.of(new StringLiteral(bucketedColumn))),
                            "bucket_count", new LongLiteral("11")),
                    session,
                    metadata,
                    ImmutableList.of());

            ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                    new SchemaTableName("tpch_bucketed", "tmp_table_" + node.getTableNameHint() + "_" + UUID.randomUUID().toString()),
                    node.getInputSymbols().stream()
                        .map(symbol -> new ColumnMetadata(symbol.getName(), types.get(symbol)))
                        .collect(toImmutableList()),
                    properties);

            Optional<NewTableLayout> stageNewTableLayout = metadata.getNewTableLayout(session, catalogName, tableMetadata);

            TableWriterNode.CreateHandle createHandle = new TableWriterNode.CreateHandle(
                    metadata.beginCreateTable(
                            session,
                            catalogName,
                            tableMetadata,
                            stageNewTableLayout),
                    tableMetadata.getTable());


            // Based on
            //      https://github.com/prestodb/presto/blob/1e7691554ef0e6a0064c77fc811b102002662cb4/presto-main/src/main/java/com/facebook/presto/sql/planner/LogicalPlanner.java#L395
            TableFinishNode commitNode = new TableFinishNode(
                    idAllocator.getNextId(),
                    gatheringExchange(idAllocator.getNextId(), LOCAL,
                            gatheringExchange(idAllocator.getNextId(), REMOTE,
                                    new TableWriterNode(
                                            idAllocator.getNextId(),
                                            node.getSource(),
                                            createHandle,
                                            symbolAllocator.newSymbol("partialrows", BIGINT),
                                            symbolAllocator.newSymbol("fragment", VARBINARY),
                                            node.getInputSymbols(),
                                            node.getInputSymbols().stream()
                                                    .map(Symbol::getName)
                                                    .collect(toImmutableList()),
                                            Optional.of(new PartitioningScheme(node.getTablePartitioning(), node.getInputSymbols())),
                                            Optional.empty(),
                                            Optional.empty()))),
                    createHandle,
                    symbolAllocator.newSymbol("rows", BIGINT),
                    Optional.empty(),
                    Optional.empty());



            List<? extends ColumnHandle> columnHandles = createHandle.getHandle().getConnectorHandle().getColumnHandles();
            verify(columnHandles.size() == node.getOutputSymbols().size());
            Map<Symbol, ColumnHandle> assignments = new HashMap<>();
            for (int i = 0; i < columnHandles.size(); i++) {
                assignments.put(node.getOutputSymbols().get(i), columnHandles.get(i));
            }

            // TODO: hmm..... extremely hack
            TableLayoutHandle promisedLayout = metadata.getPromisedTableLayoutHandleForStageTable(
                    session,
                    catalogName,
                    tableMetadata.getTable().getTableName(),
                    columnHandles.stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toImmutableList()));

            */


            // Comment(wxie): This is to explore whether we can just materialize the ExchangeNode...
            // So we will try to use all information we can get from ExchangeNode instead of TableStageNode
            ExchangeNode exchangeNode = (ExchangeNode) node.getSource();
            ExchangeTableDescriptor exchangeTableDescriptor = metadata.prepareExchangeTable(
                    session,
                    catalogName,
                    /* List<ColumnMetadata> columnMetadatas */
                    node.getInputSymbols().stream()
                            .map(symbol -> new ColumnMetadata(symbol.getName(), types.get(symbol)))
                            .collect(toImmutableList()),
                    exchangeNode.getPartitioningScheme().getPartitioning().getHandle().getConnectorHandle(),
                    /* List<String> partitionColumns */
                    exchangeNode.getPartitioningScheme().getPartitioning().getArguments().stream()
                        .map(Partitioning.ArgumentBinding::getColumn)
                        .map(Symbol::getName)
                        .collect(toImmutableList()));

            TableWriterNode.CreateHandle createHandle = new TableWriterNode.CreateHandle(exchangeTableDescriptor.outputTableHandle, exchangeTableDescriptor.tableName);

            // Based on
            //      https://github.com/prestodb/presto/blob/1e7691554ef0e6a0064c77fc811b102002662cb4/presto-main/src/main/java/com/facebook/presto/sql/planner/LogicalPlanner.java#L395
            TableFinishNode commitNode = new TableFinishNode(
                    idAllocator.getNextId(),
                    gatheringExchange(idAllocator.getNextId(), LOCAL,
                            gatheringExchange(idAllocator.getNextId(), REMOTE,
                                    new TableWriterNode(
                                            idAllocator.getNextId(),
                                            node.getSource(),
                                            createHandle,
                                            symbolAllocator.newSymbol("partialrows", BIGINT),
                                            symbolAllocator.newSymbol("fragment", VARBINARY),
                                            node.getInputSymbols(),
                                            node.getInputSymbols().stream()
                                                    .map(Symbol::getName)
                                                    .collect(toImmutableList()),
                                            Optional.of(new PartitioningScheme(node.getTablePartitioning(), node.getInputSymbols())),
                                            Optional.empty(),
                                            Optional.empty()))),
                    createHandle,
                    symbolAllocator.newSymbol("rows", BIGINT),
                    Optional.empty(),
                    Optional.empty());


            // Get table scan related stuff
            List<ColumnHandle> columnHandles = exchangeTableDescriptor.columnHandles;
            verify(columnHandles.size() == node.getOutputSymbols().size());
            Map<Symbol, ColumnHandle> assignments = new HashMap<>();
            for (int i = 0; i < columnHandles.size(); i++) {
                assignments.put(node.getOutputSymbols().get(i), columnHandles.get(i));
            }

            TableScanNode tableScanNode = new TableScanNode(
                    idAllocator.getNextId(),
                    // TableHandle is used in planning, once planning is finished, TableLayout is used to get splits: https://github.com/prestodb/presto/blob/569a811fd1c584245fc472221b0258453e0ad851/presto-main/src/main/java/com/facebook/presto/sql/planner/DistributedExecutionPlanner.java#L146-L149
                    exchangeTableDescriptor.tableHandle,
                    node.getOutputSymbols(),
                    assignments,
                    Optional.of(exchangeTableDescriptor.layoutHandle),
                    TupleDomain.all(),
                    TupleDomain.all());

            return tableScanNode.withStagedTableFinishNode(commitNode);


        }
    }

    // A "fake" connector table handle :)
    public static class StageTableHandle
            implements ConnectorTableHandle
    {
        String dumb;

        @JsonCreator
        public StageTableHandle(@JsonProperty("dumb") String dumb)
        {
            this.dumb = dumb;
        }

        @JsonProperty
        public String getDumb()
        {
            return dumb;
        }
    }
}
