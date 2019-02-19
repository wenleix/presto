package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;

import java.util.List;

public class ExchangeTableDescriptor
{
    public final TableHandle tableHandle;
    public final List<ColumnHandle> columnHandles;
    public final SchemaTableName tableName;
    public final TableLayoutHandle layoutHandle;
    public final OutputTableHandle outputTableHandle;

    public ExchangeTableDescriptor(
            TableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            SchemaTableName tableName,
            TableLayoutHandle layoutHandle,
            OutputTableHandle outputTableHandle)
    {
        this.tableHandle = tableHandle;
        this.columnHandles = columnHandles;
        this.tableName = tableName;
        this.layoutHandle = layoutHandle;
        this.outputTableHandle = outputTableHandle;
    }
}
