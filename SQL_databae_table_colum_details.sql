-- All details in a database
select * from INFORMATION_SCHEMA.COLUMNS 
--where COLUMN_NAME like '%clientid%' 
order by TABLE_NAME

-- DB name, Table name, Column name, Data Type
SELECT
    IC.TABLE_CATALOG,
    IC.TABLE_NAME,
    IC.COLUMN_NAME,
    IC.Data_TYPE,
    EP.[Value] as [MS_Description],
    IKU.CONSTRAINT_NAME, 
    ITC.CONSTRAINT_TYPE,
    IC.IS_NULLABLE
 FROM
    INFORMATION_SCHEMA.COLUMNS IC
    INNER JOIN sys.columns sc ON OBJECT_ID(QUOTENAME(IC.TABLE_SCHEMA) + '.' + QUOTENAME(IC.TABLE_NAME)) = sc.[object_id] AND IC.COLUMN_NAME = sc.name
    LEFT OUTER JOIN sys.extended_properties EP ON sc.[object_id] = EP.major_id AND sc.[column_id] = EP.minor_id AND EP.name = 'MS_Description' AND EP.class = 1 
    LEFT OUTER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE IKU ON IKU.COLUMN_NAME = IC.COLUMN_NAME and IKU.TABLE_NAME = IC.TABLE_NAME and IKU.TABLE_CATALOG = IC.TABLE_CATALOG
    LEFT OUTER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS ITC ON ITC.TABLE_NAME = IKU.TABLE_NAME and ITC.CONSTRAINT_NAME = IKU.CONSTRAINT_NAME
--WHERE IC.TABLE_CATALOG = 'softlo_int'
  --and IC.TABLE_SCHEMA = 'dbo'
  --and IC.TABLE_NAME = 'gl_pay_cancel'
order by --IC.ORDINAL_POSITION
          IC.COLUMN_NAME

-- Database names
select * from sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')

-- Record count
SELECT sc.name +'.'+ ta.name TableName
 ,SUM(pa.rows) RowCnt
 FROM sys.tables ta
 INNER JOIN sys.partitions pa
 ON pa.OBJECT_ID = ta.OBJECT_ID
 INNER JOIN sys.schemas sc
 ON ta.schema_id = sc.schema_id
 WHERE ta.is_ms_shipped = 0 AND pa.index_id IN (1,0)
 GROUP BY sc.name,ta.name
 ORDER BY SUM(pa.rows) DESC