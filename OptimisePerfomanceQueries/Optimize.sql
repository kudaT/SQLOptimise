----Identify Slow Queries-------------
SELECT 
    TOP 10 
    qs.total_elapsed_time / qs.execution_count AS AvgExecutionTime,
    qs.total_elapsed_time AS TotalExecutionTime,
    qs.execution_count,
    qs.text AS QueryText,
    qp.query_plan
FROM 
    sys.dm_exec_query_stats qs
CROSS APPLY 
    sys.dm_exec_sql_text(qs.sql_handle) AS qs
CROSS APPLY 
    sys.dm_exec_query_plan(qs.plan_handle) AS qp
ORDER BY 
    AvgExecutionTime DESC;


-----Check for Missing Indexes-------
SELECT 
    migs.avg_user_impact AS AvgImpact,
    migs.user_seeks + migs.user_scans AS UserRequests,
    mid.statement AS TableName,
    mid.equality_columns AS EqualityColumns,
    mid.inequality_columns AS InequalityColumns,
    mid.included_columns AS IncludedColumns
FROM 
    sys.dm_db_missing_index_group_stats migs
JOIN 
    sys.dm_db_missing_index_groups mig
    ON migs.group_handle = mig.index_group_handle
JOIN 
    sys.dm_db_missing_index_details mid
    ON mig.index_handle = mid.index_handle
ORDER BY 
    AvgImpact DESC;

------Identify Fragmented Indexes-------
SELECT 
    dbschemas.[name] AS SchemaName,
    dbtables.[name] AS TableName,
    dbindexes.[name] AS IndexName,
    indexstats.avg_fragmentation_in_percent
FROM 
    sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, NULL) indexstats
INNER JOIN 
    sys.tables dbtables
    ON dbtables.[object_id] = indexstats.[object_id]
INNER JOIN 
    sys.schemas dbschemas
    ON dbtables.[schema_id] = dbschemas.[schema_id]
INNER JOIN 
    sys.indexes dbindexes
    ON dbindexes.[object_id] = indexstats.[object_id] 
    AND indexstats.index_id = dbindexes.index_id
WHERE 
    indexstats.avg_fragmentation_in_percent > 10
ORDER BY 
    indexstats.avg_fragmentation_in_percent DESC;

---------Rebuild or Reorganize Indexes-------
ALTER INDEX [IndexName] ON [TableName]
REBUILD;

ALTER INDEX [IndexName] ON [TableName]
REORGANIZE;

--------Review Query Execution Plans--------
SET STATISTICS PROFILE ON;

-------Optimize Query Logic---------

---Rewrite Inefficient Queries---
SELECT FirstName, LastName 
FROM Employees;

-----Use joins efficiently (avoid Cartesian products)--
SELECT 
    Orders.OrderID, Customers.CustomerName
FROM 
    Orders
INNER JOIN 
    Customers ON Orders.CustomerID = Customers.CustomerID;


-------Monitor Blocking Queries----
SELECT 
    t1.resource_type AS BlockingType,
    t1.request_mode AS BlockingMode,
    t1.wait_duration_ms AS WaitDuration,
    t2.text AS BlockingQuery,
    t3.text AS BlockedQuery
FROM 
    sys.dm_exec_requests t1
CROSS APPLY 
    sys.dm_exec_sql_text(t1.sql_handle) t2
CROSS APPLY 
    sys.dm_exec_sql_text(t1.blocking_session_id) t3
WHERE 
    t1.blocking_session_id > 0

-------Optimize Index Usage-------
SELECT 
    OBJECT_NAME(ix.[object_id]) AS TableName,
    ix.[name] AS IndexName,
    ius.user_seeks AS Seeks,
    ius.user_scans AS Scans,
    ius.user_lookups AS Lookups
FROM 
    sys.dm_db_index_usage_stats ius
JOIN 
    sys.indexes ix
    ON ix.index_id = ius.index_id
WHERE 
    ius.user_seeks = 0 
    AND ius.user_scans = 0;
