-- 1. Total de registros, inclusive anomalias.
SELECT count(*) AS total_records FROM refined.trips;

-- 2. Início e fim em 17/06/2022 (horário local da fonte).
SELECT count(*) AS trips_june_17 FROM refined.trips
WHERE pickup_at >= timestamp '2022-06-17' AND pickup_at < timestamp '2022-06-18'
  AND dropoff_at >= timestamp '2022-06-17' AND dropoff_at < timestamp '2022-06-18';

-- 3. Mais longa em distância (milhas), incluindo todos os empates.
SELECT pickup_at::date AS pickup_day, pickup_at, dropoff_at, trip_distance
FROM refined.trips
WHERE trip_distance = (SELECT max(trip_distance) FROM refined.trips)
ORDER BY pickup_at, dropoff_at;

-- 4. Desvio populacional, quartis contínuos e distância em milhas.
SELECT avg(trip_distance) AS mean, stddev_pop(trip_distance) AS stddev_population,
       min(trip_distance) AS minimum, max(trip_distance) AS maximum,
       percentile_cont(ARRAY[0.25, 0.5, 0.75]) WITHIN GROUP (ORDER BY trip_distance) AS quartiles
FROM refined.trips;
