-- Find columns with '%Batch%ID%' of SMALLINT data type that are part of indexes
SELECT 
    SCHEMA_NAME(o.schema_id) AS SchemaName,
    OBJECT_NAME(i.object_id) AS TableName,
	concat(SCHEMA_NAME(o.schema_id),'.',OBJECT_NAME(i.object_id)) as FullTableName,
    i.name AS IndexName,
    c.name AS ColumnName,
    TYPE_NAME(c.system_type_id) AS DataType,
    i.type_desc AS IndexType
FROM 
    sys.indexes i
JOIN 
    sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN 
    sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN 
    sys.objects o ON c.object_id = o.object_id
WHERE 
    c.name LIKE '%Batch%ID%'  -- Search for columns matching the pattern
    AND TYPE_NAME(c.system_type_id) = 'smallint'  -- Ensure the column is of SMALLINT data type
    AND i.is_primary_key = 0  -- Exclude primary key indexes if you only want non-primary indexes (optional)
    AND o.type = 'U'  -- Limit to user tables (U = Table)
ORDER BY 
    SchemaName, TableName, IndexName, ColumnName;


