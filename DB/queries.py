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