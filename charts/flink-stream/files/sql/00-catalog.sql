-- =============================================================================================================
-- 00 - Catalogs
--
-- Flink has no storage of its own. A *catalog* is what tells it where table metadata lives. Two are in play:
--
--   default_catalog  in-memory, dies with the job. Every Kafka table below lives here, which is why the DDL is
--                    re-executed on every submission - it IS the schema, versioned in git rather than in a
--                    metastore.
--   iceberg          a REST catalog backed by object storage. Tables here outlive the job: another engine
--                    (Spark, Trino, DuckDB) can read exactly what this pipeline wrote.
--
-- The Iceberg catalog is never made current with USE CATALOG. Doing so would make unqualified CREATE TABLE land
-- in Iceberg, so Iceberg tables are always addressed by their full three-part name instead.
-- =============================================================================================================

CREATE CATALOG iceberg WITH (
    'type'                 = 'iceberg',
    'catalog-type'         = 'rest',
    'uri'                  = '${ICEBERG_REST_URI:-http://iceberg-rest:8181}',
    'warehouse'            = '${ICEBERG_WAREHOUSE:-s3://warehouse}',
    -- S3FileIO talks to MinIO exactly as it would to S3; only the endpoint and path-style addressing differ.
    'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'          = '${S3_ENDPOINT:-http://minio:9000}',
    's3.path-style-access' = 'true',
    's3.access-key-id'     = '${S3_ACCESS_KEY:-minioadmin}',
    's3.secret-access-key' = '${S3_SECRET_KEY:-minioadmin}'
);

CREATE DATABASE IF NOT EXISTS iceberg.lakehouse;
