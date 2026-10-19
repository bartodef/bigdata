-- Lab DuckDB : conversion du gros dataset en Parquet
.echo on
.timer on
COPY (FROM read_csv(getvariable('bucket') || '/large/orders.csv', strict_mode=false))
TO (getvariable('bucket') || '/large/orders.parquet') (FORMAT parquet);
SELECT count(*) FROM read_parquet(getvariable('bucket') || '/large/orders.parquet');
SELECT row_group_id, row_group_num_rows, stats_min, stats_max
FROM parquet_metadata(getvariable('bucket') || '/large/orders.parquet')
WHERE path_in_schema = 'date';
