from sqlalchemy import text

# -------------------------------------------------
# 1) Microplastics by Region (Top 10)
MICRO_BY_REGION_TOP10 = text("""
WITH region_avg AS (
  SELECT
    r.region,
    AVG(m.measurement) AS avg_microplastics,
    COUNT(*) AS n_samples
  FROM fact_microplastics m
  LEFT JOIN dim_region r ON m.region_id = r.region_id
  LEFT JOIN dim_date d ON m.date_id = d.date_id
  WHERE (:start_date IS NULL OR d.full_date >= :start_date)
    AND (:end_date IS NULL OR d.full_date < :end_date)
  GROUP BY r.region
)
SELECT *
FROM region_avg
WHERE region IS NOT NULL
ORDER BY avg_microplastics DESC
LIMIT 10;
""")

# -------------------------------------------------
# 2) Depth effects (Top 10)
DEPTH_BINS_EFFECT_TOP10 = text("""
WITH depth_avg AS (
  SELECT
    CASE
      WHEN m.water_sample_depth IS NULL THEN 'Unknown'
      WHEN m.water_sample_depth < 5 THEN '0-5m'
      WHEN m.water_sample_depth < 20 THEN '5-20m'
      WHEN m.water_sample_depth < 50 THEN '20-50m'
      WHEN m.water_sample_depth < 200 THEN '50-200m'
      ELSE '200m+'
    END AS depth_band,
    AVG(m.measurement) AS avg_microplastics,
    COUNT(*) AS n_samples
  FROM fact_microplastics m
  LEFT JOIN dim_date d ON m.date_id = d.date_id
  WHERE (:start_date IS NULL OR d.full_date >= :start_date)
    AND (:end_date IS NULL OR d.full_date < :end_date)
  GROUP BY depth_band
)
SELECT *
FROM depth_avg
WHERE depth_band != 'Unknown'
ORDER BY avg_microplastics DESC
LIMIT 10;
""")

# -------------------------------------------------
# 3) Critical zones (High contamination) SUM per region
CRITICAL_ZONES_HIGH = text("""
SELECT
    r.region,
    o.ocean,
    SUM(m.measurement) AS sum_measurements,
    COUNT(*) AS n_samples
FROM fact_microplastics m
LEFT JOIN dim_region r ON m.region_id = r.region_id
LEFT JOIN dim_ocean o ON m.ocean_id = o.ocean_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date IS NULL OR d.full_date < :end_date)
  AND r.region IS NOT NULL
GROUP BY r.region, o.ocean
ORDER BY sum_measurements DESC;
""")

# 4) Region hotspots (Top 10 by total measurement for boxplot)
REGION_HOTSPOTS = text("""
WITH region_totals AS (
    SELECT
        r.region,
        SUM(m.measurement) AS total_measurement
    FROM fact_microplastics m
    LEFT JOIN dim_region r ON m.region_id = r.region_id
    LEFT JOIN dim_date d ON m.date_id = d.date_id
    WHERE (:start_date IS NULL OR d.full_date >= :start_date)
      AND (:end_date IS NULL OR d.full_date < :end_date)
      AND r.region IS NOT NULL
    GROUP BY r.region
    ORDER BY total_measurement DESC
    LIMIT 10
)
SELECT
    r.region,
    m.measurement
FROM fact_microplastics m
JOIN region_totals rt ON m.region_id = (SELECT region_id FROM dim_region WHERE region = rt.region)
LEFT JOIN dim_region r ON m.region_id = r.region_id
WHERE r.region IS NOT NULL;
""")

# -------------------------------------------------
# 5) Method effects
METHOD_EFFECTS_TOP10 = text("""
SELECT
  sm.sampling_method,
  AVG(m.measurement) AS avg_microplastics,
  STDDEV_SAMP(m.measurement) AS sd_micro,
  COUNT(*) AS n_samples
FROM fact_microplastics m
LEFT JOIN dim_sampling_method sm ON m.method_id = sm.method_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date IS NULL OR d.full_date < :end_date)
GROUP BY sm.sampling_method
ORDER BY avg_microplastics DESC
LIMIT 10;
""")

# -------------------------------------------------
# 6) Concentration class by region
CONC_CLASS_BY_REGION_TOP10 = text("""
WITH region_counts AS (
    SELECT
        r.region,
        SUM(1) AS total_samples
    FROM fact_microplastics m
    LEFT JOIN dim_region r ON m.region_id = r.region_id
    LEFT JOIN dim_date d ON m.date_id = d.date_id
    WHERE (:start_date IS NULL OR d.full_date >= :start_date)
      AND (:end_date IS NULL OR d.full_date < :end_date)
      AND r.region IS NOT NULL
    GROUP BY r.region
    ORDER BY total_samples DESC
    LIMIT 20
)
SELECT
    r.region,
    c.concentration_class_text AS class_text,
    AVG(m.measurement) AS avg_measurement,
    COUNT(*) AS n_samples
FROM fact_microplastics m
JOIN region_counts rc ON m.region_id = (SELECT region_id FROM dim_region WHERE region = rc.region)
LEFT JOIN dim_region r ON m.region_id = r.region_id
LEFT JOIN dim_concentration_class c ON m.concentration_id = c.concentration_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE c.concentration_class_text IS NOT NULL
GROUP BY r.region, c.concentration_class_text
ORDER BY r.region, c.concentration_class_text;
""")

# -------------------------------------------------
# 7) Global map data
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
LEFT JOIN fact_microplastics m ON m.location_id = s.location_id
LEFT JOIN dim_region r ON m.region_id = r.region_id
LEFT JOIN dim_ocean o ON m.ocean_id = o.ocean_id
LEFT JOIN dim_location l ON s.location_id = l.location_id
LEFT JOIN dim_date d ON m.date_id = d.date_id
WHERE l.latitude IS NOT NULL
  AND l.longitude IS NOT NULL;
""")

# -------------------------------------------------

# 8) Tendencia por año (promedio, total y #muestras)
YEAR_TREND = text("""
SELECT
  d.year,
  AVG(m.measurement)  AS avg_microplastics,
  SUM(m.measurement)  AS total_microplastics,
  COUNT(*)            AS n_samples
FROM fact_microplastics m
JOIN dim_date d ON d.date_id = m.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY d.year
ORDER BY d.year;
""")

#9 Ranking por océano (total, promedio y #muestras)
OCEAN_RANKING_TOTAL = text("""
SELECT
  o.ocean,
  SUM(m.measurement) AS total_microplastics,
  AVG(m.measurement) AS avg_microplastics,
  COUNT(*)           AS n_samples
FROM fact_microplastics m
LEFT JOIN dim_ocean o ON o.ocean_id = m.ocean_id
LEFT JOIN dim_date d  ON d.date_id   = m.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY o.ocean
ORDER BY total_microplastics DESC;
""")

ORGANIZATION_ACTIVITY = text("""
SELECT
  org.organization,
  COUNT(*)           AS n_samples,
  SUM(m.measurement) AS total_microplastics,
  AVG(m.measurement) AS avg_microplastics
FROM fact_microplastics m
LEFT JOIN dim_organization org ON org.organization_id = m.organization_id
LEFT JOIN dim_date d           ON d.date_id          = m.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY org.organization
ORDER BY n_samples DESC;
""")

# 12) ZONAS CRÍTICAS – High-High (alta biodiversidad + alta contaminación)
CRITICAL_ZONES_HIGHHIGH = text("""
-- 1) Promedio por ubicación
WITH loc AS (
  SELECT
    m.location_id,
    AVG(m.measurement) AS avg_micro
  FROM fact_microplastics m
  GROUP BY m.location_id
),
pair AS (
  SELECT
    l.location_id,
    l.avg_micro,
    s.species_count,
    m.region_id
  FROM loc l
  JOIN fact_species s      ON s.location_id = l.location_id
  JOIN fact_microplastics m ON m.location_id = l.location_id
),
-- 2) Cuartiles (NTILE) para definir alto/bajo
ranked AS (
  SELECT
    p.*,
    NTILE(4) OVER (ORDER BY avg_micro)     AS q_micro,
    NTILE(4) OVER (ORDER BY species_count) AS q_species
  FROM pair p
)
SELECT
  r.region,
  COUNT(*) AS n_high_high
FROM ranked
JOIN dim_region r ON r.region_id = ranked.region_id
WHERE q_micro = 4 AND q_species = 4
GROUP BY r.region
ORDER BY n_high_high DESC;
""")

# 13) ZONAS CRÍTICAS – Low-High (baja biodiversidad + alta contaminación)
CRITICAL_ZONES_LOWBIODIV_HIGHCONT = text("""
WITH loc AS (
  SELECT m.location_id, AVG(m.measurement) AS avg_micro
  FROM fact_microplastics m
  GROUP BY m.location_id
),
pair AS (
  SELECT
    l.location_id,
    l.avg_micro,
    s.species_count,
    m.region_id
  FROM loc l
  JOIN fact_species s      ON s.location_id = l.location_id
  JOIN fact_microplastics m ON m.location_id = l.location_id
),
ranked AS (
  SELECT
    p.*,
    NTILE(4) OVER (ORDER BY avg_micro)     AS q_micro,
    NTILE(4) OVER (ORDER BY species_count) AS q_species
  FROM pair p
)
SELECT
  r.region,
  COUNT(*) AS n_low_high
FROM ranked
JOIN dim_region r ON r.region_id = ranked.region_id
WHERE q_micro = 4 AND q_species = 1
GROUP BY r.region
ORDER BY n_low_high DESC;
""")

#14 Muestras por año
SAMPLES_PER_YEAR = text("""
SELECT
  d.year,
  COUNT(*) AS n_samples
FROM fact_microplastics m
JOIN dim_date d ON d.date_id = m.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY d.year
ORDER BY d.year;
""")
#Metodos de recoleccion por año
METHODS_BY_YEAR_COUNTS = text("""
SELECT
  d.year,
  sm.sampling_method,
  COUNT(*) AS n_samples
FROM fact_microplastics m
JOIN dim_date d             ON d.date_id    = m.date_id
LEFT JOIN dim_sampling_method sm ON sm.method_id = m.method_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY d.year, sm.sampling_method
ORDER BY d.year, n_samples DESC;
""")

#15 Métodos por banda de profundidad (heatmap)
METHODS_BY_WATERSAMPLEDEPTH = text("""
SELECT
  CASE
    WHEN depth_val IS NULL THEN 'Unknown'
    WHEN depth_val < 5   THEN '0–5m'
    WHEN depth_val < 20  THEN '5–20m'
    WHEN depth_val < 50  THEN '20–50m'
    WHEN depth_val < 200 THEN '50–200m'
    ELSE '200m+'
  END AS depth_band,
  TRIM(LOWER(sm.sampling_method)) AS sampling_method,
  COUNT(*) AS n_samples
FROM (
  SELECT
    CAST(REPLACE(TRIM(
      NULLIF(TRIM(water_sample_depth), '')
    ), ',', '.') AS DOUBLE) AS depth_val,
    method_id
  FROM fact_microplastics
) m
LEFT JOIN dim_sampling_method sm 
  ON sm.method_id = m.method_id
GROUP BY depth_band, sampling_method
ORDER BY depth_band, n_samples DESC;

""")

# 16) Ranking por Entorno Marino
MARINE_SETTING_RANKING = text("""
SELECT
  ms.marine_setting,
  AVG(m.measurement) AS avg_microplastics,
  SUM(m.measurement) AS total_microplastics,
  COUNT(*)           AS n_samples
FROM fact_microplastics m
LEFT JOIN dim_marine_setting ms ON ms.marine_setting_id = m.marine_setting_id
LEFT JOIN dim_date d           ON d.date_id            = m.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
  AND ms.marine_setting IS NOT NULL
GROUP BY ms.marine_setting
ORDER BY avg_microplastics DESC
LIMIT 10;
""")

# 17) Tendencia Mensual/Estacional
MONTHLY_TREND = text("""
SELECT
  d.month,
  AVG(m.measurement) AS avg_microplastics,
  COUNT(*)           AS n_samples
FROM fact_microplastics m
JOIN dim_date d ON d.date_id = m.date_id
WHERE (:start_date IS NULL OR d.full_date >= :start_date)
  AND (:end_date   IS NULL OR d.full_date <  :end_date)
GROUP BY d.month
ORDER BY d.month;
""")
