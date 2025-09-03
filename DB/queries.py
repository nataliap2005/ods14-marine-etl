# DB/queries.py
from sqlalchemy import text

# 1) Species vs Microplastics by Region
# Promedia species_count y microplastic measurement por región (join por location_id).
SPECIES_MICRO_BY_REGION = text("""
WITH paired AS (
  SELECT
    r.region,
    s.location_id,
    AVG(s.species_count)        AS avg_species_count,
    AVG(m.measurement)          AS avg_microplastics
  FROM fact_species s
  JOIN fact_microplastics m ON m.location_id = s.location_id
  LEFT JOIN dim_region r       ON m.region_id = r.region_id
  /* Filtros opcionales por fecha (solo microplásticos tiene fecha) */
  LEFT JOIN dim_date d         ON m.date_id = d.date_id
  WHERE (:start_date IS NULL OR d.full_date >= :start_date)
    AND (:end_date   IS NULL OR d.full_date <  :end_date)
  GROUP BY r.region, s.location_id
)
SELECT
  region,
  COUNT(*)                          AS locations,
  AVG(avg_species_count)            AS region_avg_species,
  AVG(avg_microplastics)            AS region_avg_microplastics
FROM paired
GROUP BY region
ORDER BY region;
""")

# 2) Pearson correlation (overall) between Species Count and Microplastics
# Correlación global usando fórmula de Pearson con agregados en SQL.
CORR_SPECIES_MICRO_OVERALL = text("""
SELECT
  (AVG(s.species_count * m.measurement)
   - AVG(s.species_count) * AVG(m.measurement))
  / NULLIF(STDDEV_SAMP(s.species_count) * STDDEV_SAMP(m.measurement), 0) AS pearson_r
FROM fact_species s
JOIN fact_microplastics m ON m.location_id = s.location_id
LEFT JOIN dim_date d      ON m.date_id = d.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date);
""")

# 3) Species vs Concentration Class (range/text)
# Compara biodiversidad promedio por categoría de concentración.
SPECIES_BY_CONC_CLASS = text("""
SELECT
  c.concentration_class_text  AS class_text,
  c.concentration_class_range AS class_range,
  AVG(s.species_count)        AS avg_species_count,
  COUNT(*)                    AS n_samples
FROM fact_species s
JOIN fact_microplastics m ON m.location_id = s.location_id
JOIN dim_concentration_class c ON m.concentration_id = c.concentration_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY c.concentration_class_text, c.concentration_class_range
ORDER BY avg_species_count DESC;
""")

# 4) Depth effects: bin Water Sample Depth and analyze biodiversity & contamination
# Efecto de la profundidad del muestreo en especies y microplásticos.
DEPTH_BINS_EFFECT = text("""
SELECT
  CASE
    WHEN m.water_sample_depth IS NULL        THEN 'Unknown'
    WHEN m.water_sample_depth < 5            THEN '0-5m'
    WHEN m.water_sample_depth < 20           THEN '5-20m'
    WHEN m.water_sample_depth < 50           THEN '20-50m'
    WHEN m.water_sample_depth < 200          THEN '50-200m'
    ELSE '200m+'
  END AS depth_band,
  AVG(s.species_count)  AS avg_species_count,
  AVG(m.measurement)    AS avg_microplastics,
  COUNT(*)              AS n_samples
FROM fact_species s
JOIN fact_microplastics m ON m.location_id = s.location_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY depth_band
ORDER BY
  CASE depth_band
    WHEN 'Unknown' THEN 0
    WHEN '0-5m'    THEN 1
    WHEN '5-20m'   THEN 2
    WHEN '20-50m'  THEN 3
    WHEN '50-200m' THEN 4
    ELSE 5
  END;
""")

# 5) Critical zones: High biodiversity + High contamination (by region)
# Usamos NTILE(2) por región para separar alto/bajo (mitades). Requiere MySQL 8.0 (ventanas).
CRITICAL_ZONES_HIGH_HIGH = text("""
WITH paired AS (
  SELECT
    r.region,
    s.location_id,
    AVG(s.species_count) AS species_avg,
    AVG(m.measurement)   AS micro_avg
  FROM fact_species s
  JOIN fact_microplastics m ON m.location_id = s.location_id
  LEFT JOIN dim_region r ON m.region_id = r.region_id
  LEFT JOIN dim_date d ON m.date_id = d.date_id
  WHERE (:start_date IS NULL OR d.full_date >= :start_date)
    AND (:end_date   IS NULL OR d.full_date <  :end_date)
  GROUP BY r.region, s.location_id
),
ranked AS (
  SELECT
    region,
    location_id,
    species_avg,
    micro_avg,
    NTILE(2) OVER (PARTITION BY region ORDER BY species_avg DESC) AS species_half,
    NTILE(2) OVER (PARTITION BY region ORDER BY micro_avg   DESC) AS micro_half
  FROM paired
)
SELECT
  region,
  location_id,
  species_avg,
  micro_avg
FROM ranked
WHERE species_half = 1  -- high biodiversity
  AND micro_half   = 1  -- high contamination
ORDER BY region, micro_avg DESC, species_avg DESC;
""")

# 6) Critical zones: Low biodiversity + High contamination (by region)
CRITICAL_ZONES_LOW_HIGH = text("""
WITH paired AS (
  SELECT
    r.region,
    s.location_id,
    AVG(s.species_count) AS species_avg,
    AVG(m.measurement)   AS micro_avg
  FROM fact_species s
  JOIN fact_microplastics m ON m.location_id = s.location_id
  LEFT JOIN dim_region r ON m.region_id = r.region_id
  LEFT JOIN dim_date d ON m.date_id = d.date_id
  WHERE (:start_date IS NULL OR d.full_date >= :start_date)
    AND (:end_date   IS NULL OR d.full_date <  :end_date)
  GROUP BY r.region, s.location_id
),
ranked AS (
  SELECT
    region,
    location_id,
    species_avg,
    micro_avg,
    NTILE(2) OVER (PARTITION BY region ORDER BY species_avg DESC) AS species_half,
    NTILE(2) OVER (PARTITION BY region ORDER BY micro_avg   DESC) AS micro_half
  FROM paired
)
SELECT
  region,
  location_id,
  species_avg,
  micro_avg
FROM ranked
WHERE species_half = 2  -- low biodiversity
  AND micro_half   = 1  -- high contamination
ORDER BY region, micro_avg DESC;
""")

# 7) Region ranking: hotspots (biodiversity vs contamination)
# Ranking por región con z-scores simples (normalización lineal con AVG/STDDEV).
REGION_HOTSPOTS = text("""
WITH region_stats AS (
  SELECT
    r.region,
    AVG(s.species_count) AS avg_species,
    STDDEV_SAMP(s.species_count) AS sd_species,
    AVG(m.measurement)   AS avg_micro,
    STDDEV_SAMP(m.measurement)   AS sd_micro
  FROM fact_species s
  JOIN fact_microplastics m ON m.location_id = s.location_id
  LEFT JOIN dim_region r ON m.region_id = r.region_id
  LEFT JOIN dim_date d ON m.date_id = d.date_id
  WHERE (:start_date IS NULL OR d.full_date >= :start_date)
    AND (:end_date   IS NULL OR d.full_date <  :end_date)
  GROUP BY r.region
)
SELECT
  region,
  avg_species,
  avg_micro,
  (avg_species - AVG(avg_species) OVER ()) / NULLIF(STDDEV_SAMP(avg_species) OVER (),0) AS z_species,
  (avg_micro   - AVG(avg_micro)   OVER ()) / NULLIF(STDDEV_SAMP(avg_micro)   OVER (),0) AS z_micro
FROM region_stats
ORDER BY z_micro DESC, z_species DESC;
""")

# 8) Paired export (for Python analysis)
# Devuelve pares (species_count, measurement) con contexto geográfico para análisis fuera de SQL.
PAIRED_OBSERVATIONS = text("""
SELECT
  r.region,
  o.ocean,
  s.location_id,
  s.species_count,
  m.measurement,
  m.water_sample_depth,
  l.latitude,
  l.longitude,
  d.full_date
FROM fact_species s
JOIN fact_microplastics m ON m.location_id = s.location_id
LEFT JOIN dim_region r  ON m.region_id = r.region_id
LEFT JOIN dim_ocean  o  ON m.ocean_id = o.ocean_id
LEFT JOIN dim_location l ON s.location_id = l.location_id
LEFT JOIN dim_date d     ON m.date_id = d.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date);
""")

# 9) Method & mesh effects on measurement and biodiversity
METHOD_MESH_EFFECTS = text("""
SELECT
  sm.sampling_method,
  sm.method_id,
  AVG(m.measurement)      AS avg_microplastics,
  STDDEV_SAMP(m.measurement) AS sd_microplastics,
  AVG(s.species_count)    AS avg_species_count,
  COUNT(*)                AS n_samples
FROM fact_microplastics m
LEFT JOIN fact_species s ON s.location_id = m.location_id
LEFT JOIN dim_sampling_method sm ON m.method_id = sm.method_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY sm.sampling_method, sm.method_id
ORDER BY avg_microplastics DESC;
""")

# 10) Concentration class by region (matrix)
CONC_CLASS_BY_REGION = text("""
SELECT
  r.region,
  c.concentration_class_text  AS class_text,
  COUNT(*)                    AS n_samples,
  AVG(m.measurement)          AS avg_measurement,
  AVG(s.species_count)        AS avg_species_count
FROM fact_microplastics m
LEFT JOIN fact_species s ON s.location_id = m.location_id
LEFT JOIN dim_region r ON m.region_id = r.region_id
LEFT JOIN dim_concentration_class c ON m.concentration_id = c.concentration_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY r.region, c.concentration_class_text
ORDER BY r.region, avg_measurement DESC;
""")
