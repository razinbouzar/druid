SELECT col1 , LAG(col1) OVER ( PARTITION BY col3 ORDER BY col1 ) LAG_col1 FROM "fewRowsAllData.parquet"