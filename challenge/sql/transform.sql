CREATE SCHEMA IF NOT EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS refined;
DROP TABLE IF EXISTS refined.trips;
DROP TABLE IF EXISTS trusted.trips;
CREATE TABLE trusted.trips AS
SELECT source_file, source_row,
       tpep_pickup_datetime::timestamp AS pickup_at,
       tpep_dropoff_datetime::timestamp AS dropoff_at,
       trip_distance::double precision AS trip_distance,
       passenger_count::double precision AS passenger_count,
       total_amount::double precision AS total_amount
FROM raw.trips;
CREATE TABLE refined.trips AS
SELECT source_file, source_row, pickup_at, dropoff_at, trip_distance,
       passenger_count, total_amount, dropoff_at - pickup_at AS duration,
       (pickup_at IS NOT NULL AND dropoff_at >= pickup_at AND trip_distance >= 0
        AND trip_distance < 'Infinity'::double precision) AS is_valid
FROM trusted.trips;
CREATE INDEX ON refined.trips (pickup_at);
ANALYZE refined.trips;
