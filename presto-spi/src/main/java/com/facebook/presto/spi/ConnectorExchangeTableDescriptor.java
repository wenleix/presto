package com.facebook.presto.spi;

import java.util.List;

public class ConnectorExchangeTableDescriptor
{
    public final ConnectorTableHandle tableHandle;
    public final List<ColumnHandle> columnHandles;
    public final SchemaTableName tableName;
    public final ConnectorTableLayoutHandle layoutHandle;
    public final ConnectorOutputTableHandle outputTableHandle;

    public ConnectorExchangeTableDescriptor(
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            SchemaTableName tableName,
            ConnectorTableLayoutHandle layoutHandle,
            ConnectorOutputTableHandle outputTableHandle)
    {
        this.tableHandle = tableHandle;
        this.columnHandles = columnHandles;
        this.tableName = tableName;
        this.layoutHandle = layoutHandle;
        this.outputTableHandle = outputTableHandle;
    }
}

