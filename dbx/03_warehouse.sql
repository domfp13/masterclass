USE CATALOG MASTERCLASS;

-- Creating table
CREATE OR REPLACE TABLE MASTERCLASS.BRONZE.RAW_LINEITEMS_WAREHOUSE (
  V VARIANT
) USING DELTA;

-- Query some records
SELECT * FROM PARQUET.`abfss://warehouse@dbxdl.dfs.core.windows.net/lineitems/*` LIMIT 5;

-- Load data into table
COPY INTO MASTERCLASS.BRONZE.RAW_LINEITEMS_WAREHOUSE
FROM (
  SELECT parse_json(to_json(struct(*))) AS V 
  FROM 'abfss://warehouse@dbxdl.dfs.core.windows.net/lineitems/*'
)
FILEFORMAT = PARQUET
FORMAT_OPTIONS ('singleVariantColumn' = 'true');

-- Counting 59,986,052
SELECT COUNT(*) FROM raw_lineitems_warehouse;

-- Selecting 5 random values
SELECT
  variant_get(V, '$.L_ORDERKEY') AS order_key,
  variant_get(V, '$.L_PARTKEY') AS part_key,
  variant_get(V, '$.L_SUPPKEY') AS supp_key,
  variant_get(V, '$.L_LINENUMBER') AS line_number,
  variant_get(V, '$.L_QUANTITY') AS quantity,
  variant_get(V, '$.L_EXTENDEDPRICE') AS extended_price,
  variant_get(V, '$.L_DISCOUNT') AS discount,
  variant_get(V, '$.L_TAX') AS tax,
  variant_get(V, '$.L_RETURNFLAG') AS return_flag,
  variant_get(V, '$.L_LINESTATUS') AS line_status,
  variant_get(V, '$.L_SHIPDATE') AS ship_date,
  variant_get(V, '$.L_COMMITDATE') AS commit_date,
  variant_get(V, '$.L_RECEIPTDATE') AS receipt_date,
  variant_get(V, '$.L_SHIPINSTRUCT') AS ship_instruct,
  variant_get(V, '$.L_SHIPMODE') AS ship_mode,
  variant_get(V, '$.L_COMMENT') AS comment
FROM MASTERCLASS.BRONZE.RAW_LINEITEMS_WAREHOUSE
ORDER BY RAND() LIMIT 5;

-- QUERY RECORDS
SELECT
  variant_get(V, '$.L_ORDERKEY') AS order_key,
  variant_get(V, '$.L_PARTKEY') AS part_key,
  variant_get(V, '$.L_SUPPKEY') AS supp_key,
  variant_get(V, '$.L_LINENUMBER') AS line_number,
  variant_get(V, '$.L_QUANTITY') AS quantity,
  variant_get(V, '$.L_EXTENDEDPRICE') AS extended_price,
  variant_get(V, '$.L_DISCOUNT') AS discount,
  variant_get(V, '$.L_TAX') AS tax,
  variant_get(V, '$.L_RETURNFLAG') AS return_flag,
  variant_get(V, '$.L_LINESTATUS') AS line_status,
  variant_get(V, '$.L_SHIPDATE') AS ship_date,
  variant_get(V, '$.L_COMMITDATE') AS commit_date,
  variant_get(V, '$.L_RECEIPTDATE') AS receipt_date,
  variant_get(V, '$.L_SHIPINSTRUCT') AS ship_instruct,
  variant_get(V, '$.L_SHIPMODE') AS ship_mode,
  variant_get(V, '$.L_COMMENT') AS comment
FROM MASTERCLASS.BRONZE.RAW_LINEITEMS_WAREHOUSE
-- This will failed due to the premature SQL engine, the attribute needs to be casted twice
--WHERE order_key IN (
WHERE CAST(variant_get(V, '$.L_ORDERKEY') AS STRING) IN (
'30370724',
'43675749',
'46386755',
'39896960',
'51780611'
);

-- Creating table
CREATE OR REPLACE TABLE MASTERCLASS.BRONZE.LINEITEMS_WAREHOUSE
USING DELTA
AS
SELECT
  CAST(variant_get(V, '$.L_ORDERKEY') AS VARCHAR(15)) AS L_ORDERKEY,
  CAST(variant_get(V, '$.L_PARTKEY') AS VARCHAR(15)) AS L_PARTKEY,
  CAST(variant_get(V, '$.L_SUPPKEY') AS VARCHAR(15)) AS L_SUPPKEY,
  CAST(variant_get(V, '$.L_LINENUMBER') AS INT) AS L_LINENUMBER,
  CAST(variant_get(V, '$.L_QUANTITY') AS FLOAT) AS L_QUANTITY,
  CAST(variant_get(V, '$.L_EXTENDEDPRICE') AS FLOAT) AS L_EXTENDEDPRICE,
  CAST(variant_get(V, '$.L_DISCOUNT') AS FLOAT) AS L_DISCOUNT,
  CAST(variant_get(V, '$.L_TAX') AS FLOAT) AS L_TAX,
  CAST(variant_get(V, '$.L_RETURNFLAG') AS VARCHAR(30)) AS L_RETURNFLAG,
  CAST(variant_get(V, '$.L_LINESTATUS') AS VARCHAR(30)) AS L_LINESTATUS,
  CAST(variant_get(V, '$.L_SHIPDATE') AS DATE) AS L_SHIPDATE,
  CAST(variant_get(V, '$.L_COMMITDATE') AS DATE) AS L_COMMITDATE,
  CAST(variant_get(V, '$.L_RECEIPTDATE') AS DATE) AS L_RECEIPTDATE,
  CAST(variant_get(V, '$.L_SHIPINSTRUCT') AS VARCHAR(30)) AS L_SHIPINSTRUCT,
  CAST(variant_get(V, '$.L_SHIPMODE') AS VARCHAR(30)) AS L_SHIPMODE,
  CAST(variant_get(V, '$.L_COMMENT') AS VARCHAR(100)) AS L_COMMENT
FROM MASTERCLASS.BRONZE.RAW_LINEITEMS_WAREHOUSE;

SELECT * FROM MASTERCLASS.BRONZE.LINEITEMS_WAREHOUSE LIMIT 10;